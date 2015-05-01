############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from tajo_util import output_type

#Percent- Percentage
@output_type("float8")
def percent(num, total):
    return num * 100 / float(total)

#commaFormat- format a number with commas, 12345-> 12,345
@output_type("text")
def comma_format(num):
    return '{:,}'.format(num)

#concatMultiple- concat multiple words
@output_type("text")
def concat4(word1, word2, word3, word4):
    return word1 + " " + word2 + " " + word3 + " " + word4