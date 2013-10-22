#!/usr/bin/env python
# Copyright (c) 2013 Soren Hansen
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup, find_packages
import pkg_resources

setup(
    name='basicdb',
    version='0.1',
    description='Basic database service',
    long_description=pkg_resources.resource_string(__name__, "README.rst"),
    author='Soren Hansen',
    author_email='soren@linux2go.dk',
    url='http://github.com/sorenh/basicdb',
    packages=find_packages(),
    include_package_data=True,
    license='Apache 2.0',
    keywords='basicdb simpledb')
