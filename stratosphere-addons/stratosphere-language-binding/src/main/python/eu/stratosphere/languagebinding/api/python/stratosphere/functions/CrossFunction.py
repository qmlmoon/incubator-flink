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


class CrossFunction(Function.Function):
    __metaclass__ = ABCMeta

    def __init__(self):
        super(CrossFunction, self).__init__()

    def function(self):
        result = self.cross(self.iterator.next(), self.iterator.next())
        self.collector.collect(result)

    @abstractmethod
    def cross(self, value1, value2):
        pass