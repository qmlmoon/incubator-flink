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


class ProtoCollector(Collector):
    COLLECTOR_SIGNAL_DONE = -1

    def __init__(self, con):
        super(ProtoCollector, self).__init__(con)

    def collect(self, result):
        self._send_record(result)

    def send_signal(self, signal):
        self._send_size(signal)

    # Sends the the given size
    def _send_size(self, given_size):
        size = ProtoTuple_pb2.TupleSize()
        size.value = given_size
        self.connection.send(size.SerializeToString())

    def _send_record(self, record):
        converter_record = ProtoConversion.convert_python_to_proto(record)
        serialized_record = converter_record.SerializeToString()
        self._send_size(len(serialized_record))
        self.connection.send(serialized_record)

