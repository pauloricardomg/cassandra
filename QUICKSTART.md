<!--
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
-->

# Quick Start Development Guide

## Setting up the project (WSL/Debian/Ubuntu)

1. Clone Cassandra project with:
```bash
$ git clone https://github.com/apache/cassandra.git
```

2. Install Cdependencies with:
```bash
$ sudo apt-get install ant openjdk-11-jdk-headless
```

3. Set the following environment variable to ensure the project compiles correctky with JDK11:
```bash
$ export CASSANDRA_USE_JDK11=true
```

4. Build the project with:
```bash
$ cd /path/to/cassandra
/path/to/cassandra$ ant build

[...]
BUILD SUCCESSFUL
Total time: 35 seconds
```

## Configuring IDE (Intellij Idea)

*Note: this guide assumes you downloaded and installed IntelliJ IDEA*

1. Create the intellij Cassandra project with the following command:
```bash
/path/to/cassandra$ ant generate-idea-files

generate-idea-files:
    [mkdir] Created dir: /path/to/cassandra/.idea
    [mkdir] Created dir: /path/to/cassandra/.idea/libraries
     [copy] Copying 9 files to /path/to/cassandra/.idea
     [copy] Copying 1 file to /path/to/cassandra

_maybe_update_idea_to_java11:

BUILD SUCCESSFUL
Total time: 5 seconds
```

2. Open Intellij and "Open" the cassandra folder (/path/to/cassandra).

*Note: do not "Create New Project", but rather "Open existing project"*

3. After the project is loaded, Use `CTRL/CMD + SHIFT + N` to open  `RepairTest` class
  1. Check if the class is compiled successfully.
  2. If you get "JDK11 is missing" error message, click "configure" and "Add new JDK" and point to a JDK, usually located in `/usr/lib/jvm/jdk_install/`.
  3. Right-click in the `RepairTest` class name and click "Run RepairTest".

## Developing your first "Hello World" patch

Let's prepare a simple patch that logs "Hello World!" during Cassandra initialization.

1.  Use the shortcut `CTRL/CMD + SHIFT + N` to open  `CassandraDaemon` class
2.  Locate the `logSystemInfo()` method with the shortcut `CTRL/CMD + ALT + SHIFT + N`
3. Add the following log statement to this method:
```java
logger.info("Hello World!");
```

## Testing changes locally with docker

Use the instructions below to spin up a local Cassandra container to test your changes.

*Note: this guide assumes you downloaded and installed docker in your local machine.*

### Publish local docker image

Use the following script to build a local "cassandra-test" docker image based on the contents of your local Cassandra workspace.
```bash
/path/to/cassandra$ docker/build.sh
[...]
Successfully built 1219e0abf6ff
Successfully tagged apache/cassandra-test:latest
```

### Start local Cassandra container

Start the "cassandra-test" container with:
```bash
/path/to/cassandra$ docker/start.sh
[...]
waiting for cassandra container...
waiting for cassandra container...
Cassandra container started with id bbd48860922e48c0759581d2f598378834bc9006e46217cf7714133c513b0a98 and ip 172.17.0.2
```

### Inspect cluster with nodetool and CQLSH

Use the following commands to run nodetool and CQLSH:

```bash
/path/to/cassandra$ docker exec -it cassandra-server nodetool status
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load        Tokens  Owns (effective)  Host ID                               Rack
UN  172.17.0.2  105.72 KiB  16      100.0%            6915b345-1f2b-4261-89e6-9d23a65c4233  rack1

/path/to/cassandra$ docker exec -it cassandra-server cqlsh
Connected to Test Cluster at 127.0.0.1:9042
[cqlsh 6.2.0 | Cassandra 4.2-SNAPSHOT | CQL spec 3.4.6 | Native protocol v5]
Use HELP for help.
cqlsh> DESCRIBE KEYSPACES

system       system_distributed  system_traces  system_virtual_schema
system_auth  system_schema       system_views

cqlsh> exit
```

### Test the "Hello World" patch in the local Cassandra container

Verify that the "Hello World" message is being print during node initialization:
```bash
/path/to/cassandra$ docker logs cassandra-server | grep "Hello World"
INFO  [main] 2022-09-15 22:09:34,075 CassandraDaemon.java:634 - Hello World!
```

### Stop local docker container

Use the following command to stop the image:
```bash
/path/to/cassandra$ docker/stop.sh
cassandra-server
```