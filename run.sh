#!/bin/sh

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

if [ "x$CASSANDRA_HOME" != "x" ]; then
    CASSANDRA_INCLUDE=$CASSANDRA_HOME/bin/cassandra.in.sh
fi

if [ "x$CASSANDRA_INCLUDE" = "x" ]; then
    for include in "`dirname "$0"`/cassandra.in.sh" \
                   "$HOME/.cassandra.in.sh" \
                   /usr/share/cassandra/cassandra.in.sh \
                   /usr/local/share/cassandra/cassandra.in.sh \
                   /opt/cassandra/cassandra.in.sh; do
        if [ -r "$include" ]; then
            . "$include"
            break
        fi
    done
elif [ -r "$CASSANDRA_INCLUDE" ]; then
    . "$CASSANDRA_INCLUDE"
fi


# Use JAVA_HOME if set, otherwise look for java in PATH
if [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA="`which java`"
fi

if [ -z "$CLASSPATH" ]; then
    echo "You must set the CLASSPATH var" >&2
    exit 1
fi

set +x

# For Cassandra 4.1

java -Dcassandra.storagedir=$CASSANDRA_HOME/data -Dcassandra.config=file:///$CASSANDRA_HOME/conf/cassandra.yaml \
    -cp "$CLASSPATH./impl/target/ttl-remover.jar:./cassandra-4.1/target/ttl-remover-cassandra-4.1.jar" \
    $JVM_OPTS \
    com.instaclustr.cassandra.ttl.cli.TTLRemoverCLI \
    --cassandra-version=4 \
    --sstables \
    /tmp/original-4/test/test \
    --output-path \
    /tmp/stripped \
    --cql \
    'CREATE TABLE IF NOT EXISTS test.test (id uuid, name text, surname text, PRIMARY KEY (id)) WITH default_time_to_live = 10;'

# For Cassandra 3 and 4.0

# change versions of jars on classpath to target 3 or 4
# change --cassandra-version if necessary
#java -javaagent:./buddy-agent/target/byte-buddy-agent.jar \
#    -cp "$CLASSPATH./impl/target/ttl-remover.jar:./cassandra-3/target/ttl-remover-cassandra-3.jar" \
#    $JVM_OPTS \
#    com.instaclustr.cassandra.ttl.cli.TTLRemoverCLI \
#    --cassandra-version=3 \
#    --sstables \
#    /tmp/original-3/test/test \
#    --output-path \
#    /tmp/stripped \
#    --cql \
#    'CREATE TABLE IF NOT EXISTS test.test (id uuid, name text, surname text, PRIMARY KEY (id)) WITH default_time_to_live = 10;'

# For Cassandra 2

#java -cp "$CLASSPATH./impl/target/ttl-remover.jar:./cassandra-2/target/ttl-remover-cassandra-2.jar" $JVM_OPTS \
#    com.instaclustr.cassandra.ttl.cli.TTLRemoverCLI \
#    --cassandra-version=2 \
#    --sstables \
#    /tmp/sstables2/test \
#    --output-path \
#    /tmp/stripped \
#    --cassandra-yaml \
#    $CASSANDRA_HOME/conf/cassandra.yaml \
#    --cassandra-storage-dir \
#    $CASSANDRA_HOME/data
