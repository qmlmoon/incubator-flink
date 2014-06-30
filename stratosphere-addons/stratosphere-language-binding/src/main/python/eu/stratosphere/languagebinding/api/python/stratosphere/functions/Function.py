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

from stratosphere.context import RuntimeContext
from stratosphere.connection import Connection
from stratosphere.connection import Collector
from stratosphere.connection import Iterator


class Function(object):
    __metaclass__ = ABCMeta

    def __init__(self, mode=Iterator.ProtoIterator.ITERATOR_MODE_DEF):
        self._mode = mode
        self.connection = Connection.STDPipeConnection()
        self.iterator = Iterator.ProtoIterator(self.connection, self._mode)
        self.collector = Collector.ProtoCollector(self.connection)
        self.context = RuntimeContext.RuntimeContext(self.iterator, self.collector)

    def run(self):
        while self.iterator.next():
            self.function()

    @abstractmethod
    def function(self):
        pass
