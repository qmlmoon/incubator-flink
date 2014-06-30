# #####################################################################################################################
# Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
######################################################################################################################
import threading
from abc import ABCMeta, abstractmethod
from collections import deque

from stratosphere.connection import ProtoConversion
from stratosphere.proto import ProtoTuple_pb2

class Iterator(object):
    __metaclass__ = ABCMeta

    def __init__(self, con):
        self.connection = con

    @abstractmethod
    def has_next(self):
        """
        Provides information whether this iterator contains another element.

        :return: True, if this iterator contains another element, false otherwise.
        """
        pass

    @abstractmethod
    def next(self):
        """
        Returns the next element in this iterator.

        :return: The next element.
        """
        pass

    @abstractmethod
    def all(self):
        """
        Returns all remaining elements in this iterator.
        :return: A list containing all remaining elements.
        """
        pass


class ProtoIterator(Iterator):
    #Signal indicating that all records were sent.
    ITERATOR_SIGNAL_DONE = -1
    #Flag indicating that no data has been read.
    ITERATOR_FLAG_INIT = -5
    #Default mode - Receive a known number of records (usually 1 or 2) and return one record.
    ITERATOR_MODE_DEF = 0
    #GroupReduce mode - Receive an unknown number of records of a single group and return any number of records.
    ITERATOR_MODE_GR = 1
    #CoGroup mode - Receive an unknown number of records of two different group and return any number of records.
    ITERATOR_MODE_CG = 2

    def __init__(self, con, mode):
        super(ProtoIterator, self).__init__(con)
        self.size_received = False
        self.last_size = ProtoIterator.ITERATOR_FLAG_INIT
        self.mode = mode
        self.cache = deque()
        #The following properties should never be accessed directly.
        if self.mode == ProtoIterator.ITERATOR_MODE_CG:
            #buffers to store values, differentiated by group
            self.values_a = []
            self.values_b = []
            #boolean flag indicating whether all records of the respective group have been read
            self.done_with_a = False
            self.done_with_b = False
            #start thread that receives records in the background
            self.lock = threading.Condition()
            self.thread = Receiver(self.connection, self)
            self.thread.start()

    #Resets this Iterator to resemble a newly instantiated one.
    def _reset(self):
        self.last_size = ProtoIterator.ITERATOR_FLAG_INIT
        self.size_received = False
        if self.mode == ProtoIterator.ITERATOR_MODE_CG:
            #buffers to store values, differentiated by group
            self.thread.join()
            self.values_a = []
            self.values_b = []
            #boolean flag indicating whether all records of the respective group have been read
            self.done_with_a = False
            self.done_with_b = False
            #start thread that receives records in the background
            self.lock = threading.Condition()
            self.thread = Receiver(self.connection, self)
            self.thread.start()

    #Returns a list of all remaining elements in this iterator.
    def all(self, group=-1):
        values = []
        while len(self.cache) > 0:
            values += self.cache.popleft()
        if group == -1:
            while self.has_next():
                values.append(self.next())
        else:
            while self.has_next(group):
                values.append(self.next(group))
        return values

    #Returns the next element in this iterator.
    def next(self, group=0):
        if len(self.cache) > 0:
            return self.cache.popleft()
        if not self.size_received:
            self.has_next(group, True)
        self.size_received = False
        if self.mode == ProtoIterator.ITERATOR_MODE_CG:
            return self._next_cogroup(group)
        record = self._read_record()
        return record

    #This version of next differentiates between two groups.
    def _next_cogroup(self, group):
        self.lock.acquire()
        if group == 0:
            while (not self.done_with_a) and (len(self.values_a) == 0):
                self.lock.wait()
            value = self.values_a.pop(0)
            self.lock.notify_all()
            self.lock.release()
            return value
        if group == 1:
            while (not self.done_with_b) and (len(self.values_b) == 0):
                self.lock.wait()
            value = self.values_b.pop(0)
            self.lock.notify_all()
            self.lock.release()
            return value
        raise ValueError("Invalid group identifier passed to next. Expected: 0/1 Actual: " + str(group))

    #Returns a boolean value indicating whether this iterator contains another element.
    def has_next(self, group=0, val=False):
        if not val:
            self.size_received = True
        if self.last_size == ProtoIterator.ITERATOR_FLAG_INIT:
            return self._has_next_initial()
        if self.mode == ProtoIterator.ITERATOR_MODE_CG:
            return self._has_next_cogroup(group)
        else:
            self.last_size = self._read_size()
            return not self.last_size == ProtoIterator.ITERATOR_SIGNAL_DONE

    #This version of has_next is used for the very first call.
    def _has_next_initial(self):
        if not self.mode == ProtoIterator.ITERATOR_MODE_CG:
            self.last_size = self._read_size()
        return not self.last_size == ProtoIterator.ITERATOR_SIGNAL_DONE

    #This version of has_next differentiates between two groups.
    def _has_next_cogroup(self, group):
        self.lock.acquire()
        if group == 0:
            #wait until receiver has received the size of the next record (and potentially the end signal)
            while len(self.values_a) == 0 and not self.done_with_a:
                self.lock.wait()
            res = not ((len(self.values_a) == 0) & self.done_with_a)
            self.lock.notify_all()
            self.lock.release()
            return res
        else:  #wait until receiver has received the size of the next record (and potentially the end signal)
            while len(self.values_b) == 0 and not self.done_with_b:
                self.lock.wait()
            res = not ((len(self.values_b) == 0) & self.done_with_b)
            self.lock.notify_all()
            self.lock.release()
            return res

    #Reads the size of the next record. Should not be called directly.
    def _read_size(self):
        size = ProtoTuple_pb2.TupleSize()
        size_buf = self.connection.receive(5)
        size.ParseFromString(size_buf)
        return size.value

    #Reads the next record. Should not be called directly.
    def _read_record(self):
        if self.last_size == -2:
            return -2
        if self.last_size == -1:
            return None
        raw_data = self.connection.receive(self.last_size)
        parsed_data = ProtoTuple_pb2.ProtoTuple()
        parsed_data.ParseFromString(raw_data)
        return ProtoConversion.convert_proto_to_python(parsed_data)


#Special object used by a ProtoIterator for GroupReduce functions to receive data for 2 different groups.
class Receiver(threading.Thread):
    def __init__(self, connection, iterator):
        threading.Thread.__init__(self)
        self.connection = connection
        self.iterator = iterator

    def _collect10a(self):
        for x in range(0, 10):
            self.iterator.last_size = self.iterator._read_size()
            if self.iterator.last_size == -2:
                tmp_size = self.iterator.last_size
                self.iterator.last_size = self.iterator._read_size()
                self.iterator.cache.append(self.read_record()[1])
                self.iterator.last_size = tmp_size
                self.iterator.last_size = self.iterator._read_size()
            self.iterator.lock.acquire()
            if self.iterator.last_size == ProtoIterator.ITERATOR_SIGNAL_DONE:
                self.iterator.done_with_a = True
                self.iterator.lock.notify_all()
                self.iterator.lock.release()
                break
            record = self.iterator._read_record()
            self.iterator.values_a.append(record)
            self.iterator.lock.notify_all()
            self.iterator.lock.release()

    def _collect10b(self):
        for x in range(0, 10):
            self.iterator.last_size = self.iterator._read_size()
            if self.iterator.last_size == -2:
                tmp_size = self.iterator.last_size
                self.iterator.last_size = self.iterator._read_size()
                self.iterator.cache.append(self.read_record()[1])
                self.iterator.last_size = tmp_size
                self.iterator.last_size = self.iterator._read_size()
            self.iterator.lock.acquire()
            if self.iterator.last_size == ProtoIterator.ITERATOR_SIGNAL_DONE:
                self.iterator.done_with_b = True
                self.iterator.lock.notify_all()
                self.iterator.lock.release()
                break
            record = self.iterator._read_record()
            self.iterator.values_b.append(record)
            self.iterator.lock.notify_all()
            self.iterator.lock.release()

    def run(self):
        while (not self.iterator.done_with_a) or (not self.iterator.done_with_b):
            if not self.iterator.done_with_a:
                self._collect10a()
            if not self.iterator.done_with_b:
                self._collect10b()


class Dummy(Iterator):
    def __init__(self, iterator, group):
        super(Dummy, self).__init__(iterator.connection)
        self.iterator = iterator
        self.group = group

    def next(self):
        return self.iterator.next(self.group)

    def has_next(self):
        return self.iterator.has_next(self.group)

    def all(self):
        return self.iterator.all(self.group)
