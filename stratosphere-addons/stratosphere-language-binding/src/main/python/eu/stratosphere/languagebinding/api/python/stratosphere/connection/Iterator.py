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
# #####################################################################################################################
from abc import ABCMeta, abstractmethod
from collections import deque
import struct

from stratosphere.utilities.Switch import Switch
from stratosphere.connection.RawConstants import Constants


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

    @abstractmethod
    def _reset(self):
        pass


class RawIterator(Iterator):
    def __init__(self, con):
        super(RawIterator, self).__init__(con)
        self.cache = deque()
        self.cache_mode = 0
        self.was_last0 = False
        self.was_last1 = False

    def next(self, group=0):
        if len(self.cache) > 0 & group == self.cache_mode:
            return self.cache.popleft()
        raw_meta = "\x00\x00\x00" + self.connection.receive(1)
        meta = struct.unpack(">i", raw_meta)[0]
        if meta == 64:
            if group==0:
                self.was_last0 = True
            else:
                self.was_last1 = True
            return None
        if meta == 128:
            return True
        record_group = meta >> 7
        if record_group == group:
            if (meta & 32) == 32:
                if group == 0:
                    self.was_last0 = True
                else:
                    self.was_last1 = True
            size = meta & 31
            if size == 0:
                return self._receive_field()
            result = ()
            for i in range(size):
                result += (self._receive_field(),)
            return result
        else:
            if (meta & 32) == 32:
                if group == 0:
                    self.was_last0 = True
                else:
                    self.was_last1 = True
            size = meta & 31
            if size == 0:
                self.cache.append(self._receive_field())
            else:
                result = ()
                for i in range(size):
                    result += (self._receive_field(),)
                if len(self.cache) == 0:
                    self.cache_mode = group
                self.cache.append(result)
            return self.next(group)

    def _receive_field(self):
        raw_type = "\x00\x00\x00" + self.connection.receive(1)
        type = struct.unpack(">i", raw_type)[0]
        if (type==Constants.TYPE_BOOLEAN):
            raw_bool = self.connection.receive(1)
            return struct.unpack(">?", raw_bool)[0]
        elif (type==Constants.TYPE_BYTE):
            raw_byte = self.connection.receive(1)
            return struct.unpack(">c", raw_byte)[0]
        elif (type==Constants.TYPE_FLOAT):
            raw_float = self.connection.receive(4)
            return struct.unpack(">f", raw_float)[0]
        elif (type==Constants.TYPE_DOUBLE):
            raw_double = self.connection.receive(8)
            return struct.unpack(">d", raw_double)[0]
        elif (type==Constants.TYPE_SHORT):
            raw_short = self.connection.receive(2)
            return struct.unpack(">h", raw_short)[0]
        elif (type==Constants.TYPE_INTEGER):
            raw_int = self.connection.receive(4)
            return struct.unpack(">i", raw_int)[0]
        elif (type==Constants.TYPE_LONG):
            raw_long = self.connection.receive(8)
            return struct.unpack(">l", raw_long)[0]
        elif (type==Constants.TYPE_STRING):
            raw_size = self.connection.receive(4)
            size = struct.unpack(">i", raw_size)[0]
            if size == 0:
                return ""
            return self.connection.receive(size)
        elif (type==Constants.TYPE_NULL):
            return None

    def all(self, group=0):
        values = []
        if self.cache_mode == group:
            while len(self.cache) > 0:
                values.append(self.cache.popleft())
        while self.has_next():
            values.append(self.next())
        return values

    def has_next(self, group=0):
        if group == 0:
            return not self.was_last0
        else:
            return not self.was_last1

    def _reset(self):
        self.was_last0 = False
        self.was_last1 = False


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

    def _reset(self):
        self.iterator._reset()
