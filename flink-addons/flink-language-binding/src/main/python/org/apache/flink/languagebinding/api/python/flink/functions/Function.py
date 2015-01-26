# ###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from abc import ABCMeta, abstractmethod
import dill
import sys
from collections import deque
from flink.connection import Connection, Iterator, Collector
from flink.functions import RuntimeContext


class Function(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self._connection = None
        self._iterator = None
        self._collector = None
        self.context = None
        self._chain_operator = None
        self._meta = None

    def _configure_chain(self, collector):
        if self._chain_operator is not None:
            frag = self._meta.split("|")
            self._chain_operator = self._chain_operator.replace(b"__main__", b"plan", 50)
            exec("from plan import " + frag[1])
            self._collector = dill.loads(self._chain_operator)
            self._collector._configure_chain(collector)
            self._collector.open()
        else:
            self._collector = collector

    def _chain(self, operator, meta):
        self._chain_operator = operator
        self._meta = meta

    def configure(self, input_file, output_file, port):
        self._connection = Connection.BufferingUDPMappedFileConnection(input_file, output_file, port)
        self._iterator = Iterator.Iterator(self._connection)
        self.context = RuntimeContext.RuntimeContext(self._iterator, self._collector)
        self._configure_chain(Collector.Collector(self._connection))

    @abstractmethod
    def run(self):
        pass

    def open(self):
        pass

    def close(self):
        self._collector.close()

    def go(self):
        self.receive_broadcast_variables()
        self.run()

    def receive_broadcast_variables(self):
        broadcast_count = self._iterator.next()
        self._iterator._reset()
        self._connection.reset()
        for _ in range(broadcast_count):
            name = self._iterator.next()
            self._iterator._reset()
            self._connection.reset()
            bc = deque()
            while(self._iterator.has_next()):
                bc.append(self._iterator.next())
            self.context._add_broadcast_variable(name, bc)
            self._iterator._reset()
            self._connection.reset()



