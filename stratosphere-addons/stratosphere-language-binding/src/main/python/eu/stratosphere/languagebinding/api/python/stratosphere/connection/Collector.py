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
from abc import ABCMeta, abstractmethod

from stratosphere.connection import ProtoConversion
from stratosphere.proto import ProtoTuple_pb2
import struct
from stratosphere.connection.RawConstants import Constants
class Collector(object):
    __metaclass__ = ABCMeta

    def __init__(self, con):
        self.connection = con

    @abstractmethod
    def collect(self, value):
        """
        Emits a record.

        :param value: The record to collect.:
        """
        pass

    @abstractmethod
    def _send_end_signal(self):
        pass


class RawCollector(Collector):
    def __init__(self, con):
        super(RawCollector, self).__init__(con)
        self.cache = None

    def collect(self, value):
        if self.cache is None:
            self.cache = value
        else:
            self._send_record(False)
            self.cache = value
        pass

    def _send_end_signal(self):
        self.connection.send(64)

    def finish(self):
        self._send_record(True)

    def _send_record(self, last):
        meta = 0
        if last:
            meta |= 32
        if not isinstance(self.cache, (list, tuple)):
            meta = struct.pack(">i", meta)
            self.connection.send(meta[3])
            self._send_field(self.cache)
        else:
            for field in self.cache:
                self._send_field(field)

    def _send_field(self, value):
        if isinstance(value, basestring):
            type = struct.pack(">b",Constants.TYPE_STRING)
            self.connection.send(type)
            self.connection.send(value)
        elif isinstance(value, bool):
            type = struct.pack(">b",Constants.TYPE_BOOLEAN)
            self.connection.send(type)
            data = struct.pack(">?", value)
            self.connection.send(data)
        elif isinstance(value, int):
            type = struct.pack(">b",Constants.TYPE_INTEGER)
            self.connection.send(type)
            data = struct.pack(">i", value)
            self.connection.send(data)
        elif isinstance(value, long):
            type = struct.pack(">b",Constants.TYPE_LONG)
            self.connection.send(type)
            data = struct.pack(">l", value)
            self.connection.send(data)
        elif isinstance(value, float):
            type = struct.pack(">b",Constants.TYPE_DOUBLE)
            self.connection.send(type)
            data = struct.pack(">d", value)
            self.connection.send(data)
        else:
            type = struct.pack(">b",Constants.TYPE_STRING)
            self.connection.send(type)
            self.connection.send(str(value))