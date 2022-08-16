#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Fail on error
set -e

# Keep track of initial dir
INITIAL_DIR=`pwd`

# Go to cassandra directory if not there yet
if [ ! -f build.xml ]; then
  cd ..
fi

# Build artifacts
ant artifacts -D"no-javadoc=true" -D"ant.gen-doc.skip=true"

# Copy tarball to docker dir
TARBALL_DIR=`ant -q -S echo-tarball-location`
cp $TARBALL_DIR docker/cassandra-bin.tgz

# Build image
docker build -t apache/cassandra-test docker

# Go back to initial dir
cd $INITIAL_DIR
