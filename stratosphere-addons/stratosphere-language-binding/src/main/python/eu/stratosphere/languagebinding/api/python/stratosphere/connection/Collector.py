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
        self.init = True
        self.cache = None

    def collect(self, value):
        if self.init:
            self.cache = value
            self.init = False
        else:
            self._send_record(False)
            self.cache = value

    def _send_end_signal(self):
        signal = struct.pack(">i", 64)
        self.connection.send(signal[3])

    def finish(self):
        if not self.init:
            self._send_record(True)
            self.init = True
        else:
            self._send_end_signal()

    def _send_record(self, last):
        meta = 0
        if last:
            meta |= 32
        if not isinstance(self.cache, (list, tuple)):
            meta = struct.pack(">i", meta)
            self.connection.send(meta[3])
            self._send_field(self.cache)
        else:
            meta += len(self.cache)
            meta = struct.pack(">i", meta)
            self.connection.send(meta[3])
            for field in self.cache:
                self._send_field(field)

    def _send_field(self, value):
        if value is None:
            type = struct.pack(">b", Constants.TYPE_NULL)
            self.connection.send(type)
        elif isinstance(value, basestring):
            type = struct.pack(">b", Constants.TYPE_STRING)
            size = struct.pack(">i", len(value))
            self.connection.send("".join([type, size, value]))
        elif isinstance(value, bool):
            type = struct.pack(">b", Constants.TYPE_BOOLEAN)
            data = struct.pack(">?", value)
            self.connection.send("".join([type, data]))
        elif isinstance(value, int):
            type = struct.pack(">b", Constants.TYPE_INTEGER)
            data = struct.pack(">i", value)
            self.connection.send("".join([type, data]))
        elif isinstance(value, long):
            type = struct.pack(">b", Constants.TYPE_LONG)
            data = struct.pack(">l", value)
            self.connection.send("".join([type, data]))
        elif isinstance(value, float):
            type = struct.pack(">b", Constants.TYPE_DOUBLE)
            data = struct.pack(">d", value)
            self.connection.send("".join([type, data]))
        else:
            type = struct.pack(">b", Constants.TYPE_STRING)
            size = struct.pack(">i", len(value))
            self.connection.send("".join([type, size, value]))