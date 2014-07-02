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


class JoinFunction(Function.Function):
    __metaclass__ = ABCMeta

    def __init__(self):
        super(JoinFunction, self).__init__()

    def run(self):
        while True:
            value = self.iterator.next()
            if value is None:
                break
            result = self.join(value, self.iterator.next())
            self.collector.collect(result)
            self.collector.finish()

    @abstractmethod
    def join(self, value1, value2):
        pass