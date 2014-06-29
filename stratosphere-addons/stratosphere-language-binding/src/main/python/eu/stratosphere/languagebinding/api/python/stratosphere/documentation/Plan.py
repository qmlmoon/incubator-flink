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
#=======================================================================================================================
from stratosphere.plan.Environment import get_environment
from stratosphere.plan.InputFormat import TextInputFormat
from stratosphere.plan.OutputFormat import PrintingOutputFormat
from stratosphere.plan.Constants import Types
from stratosphere.functions.MapFunction import MapFunction
import re


#1)
class Splitter(MapFunction):
    def map(self, value):
        split_input = re.split("\W+", input.lower())
        return len(split_input)


#2)
env = get_environment()
#3)
data = env.create_input(TextInputFormat("/test.txt"))
#4)
mapped_data = data.map(Splitter(), Types.INT)
#5)
mapped_data.output(PrintingOutputFormat())
#6)
env.execute()
#=======================================================================================================================
"""
Python Plan:
This is a basic example showing a plan written in python.

General structure:
The plan consists of 5 parts:
1) the user-defined function(s), contained in a class that inherits from the corresponding generic operator class
2) a call to get_environment()
3) loading data using input formats
4) manipulating the data using functions
5) outputting data using output formats
6) executing the plan

Functions and formats may require you to specify the output type.
mapped_data = data.map(Splitter(), Types.INT)
    returns a set containing ints.
mapped_data = data.map(Splitter(), [Types.INT])
    returns a set of tuples containing a single int.
mapped_data = data.map(Splitter(), [Types.INT, Types.STRING])
    returns a set of tuples containing an int and a string

Whether this is necessary can be determined from the signature. Note that currently only basic python types are
fully supported, other types will be converted to strings. This has to be accounted for when specifying the types.

To submit a plan to stratosphere, call ./bin/pysphere.sh, along with the path to
your package containing all files related to your program, as well as the path to the python file containing the plan.
e.g.: ./bin/pysphere.sh /path/to/package /path/to/plan

Hybrid Plans:
If the plan is written in Java then every function has to be contained in it's own python script. The above function
would then look like this:
"""
#=======================================================================================================================
from stratosphere.functions.MapFunction import MapFunction
import re

class Splitter(MapFunction):
    def map(self, value):
        split_input = re.split("\W+", input.lower())
        return len(split_input)

Splitter().run()
#=======================================================================================================================
"""
Function Arguments:

Several functions receive iterators instead of simple objects, and collectors instead of having a return type.

Iterators support the following operations:
has_next(): returns a boolean value indicating whether another values is available. Note that for CoGroup functions,
the first call will always return true.
next(): returns the next value.
all(): returns all values in the iterator as a list.

Collectors support the following operations:
collect(): collects a single value.
"""

