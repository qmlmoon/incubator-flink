################################################################################
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

from flink.functions import Function


class FlatMapFunction(Function.Function):
    __metaclass__ = ABCMeta

    def __init__(self):
        super(FlatMapFunction, self).__init__()

    def run(self):
        while 1:
            value = self._iterator.next()
            if value is None:
                break
            self.flat_map(value, self._collector)
            self._collector._finish()

    @abstractmethod
    def flat_map(self, value, collector):
        pass