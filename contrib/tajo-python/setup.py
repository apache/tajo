# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup, find_packages
import sys

DESCRIPTION = """python tajo rest client
"""

install_requires = ["httplib2"]

setup(
    name="tajo-rest-client",
    version="0.0.1",
    description="a Python implementation of Tajo Rest Client",
    long_description=DESCRIPTION,
    url='http://github.com/charsyam/python-tajo-rest-client/',
    author='DaeMyung Kang',
    author_email='charsyam@gmail.com',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=install_requires,
    test_suite='',
)
