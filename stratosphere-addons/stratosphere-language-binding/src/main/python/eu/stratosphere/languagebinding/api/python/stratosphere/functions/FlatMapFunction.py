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

from stratosphere.functions import Function
from stratosphere.connection import Iterator

class FlatMapFunction(Function.Function):
    __metaclass__ = ABCMeta

    def __init__(self):
        super(FlatMapFunction, self).__init__()

    def run(self):
        while True:
            value = self.iterator.next()
            if value is None:
                break
            self.flat_map(value, self.collector)
            self.collector.finish()

    @abstractmethod
    def flat_map(self, value, collector):
        pass
