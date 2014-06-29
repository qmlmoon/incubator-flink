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
from stratosphere.plan.Environment import get_environment
from stratosphere.plan.Constants import Types
from stratosphere.functions.FlatMapFunction import FlatMapFunction
from stratosphere.functions.GroupReduceFunction import GroupReduceFunction
import re


class Tokenizer(FlatMapFunction):
    def flat_map(self, value, collector):
        words = re.split("\W+", value.lower())
        for word in words:
            collector.collect((1, word))


class Adder(GroupReduceFunction):
    def group_reduce(self, iterator, collector):
        first = iterator.next()
        count = first[0]
        word = first[1]

        while iterator.has_next():
            count += iterator.next()[0]

        collector.collect((count, word))

if __name__ == "__main__":
    env = get_environment()
    data = env.read_text("hdfs:/datasets/enwiki-latest-pages-meta-current.xml")

    data\
        .flatmap(Tokenizer(),(Types.INT, Types.STRING))\
        .group_by(1)\
        .groupreduce(Adder(), (Types.INT, Types.STRING))\
        .write_csv("hdfs:/tmp/python/output")

    env.set_degree_of_parallelism(104)

    env.execute()