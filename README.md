# TTLRemover
SSTable TTLRemover


### Usage

This tool is implemented based on Cassandra 2.2.

#### Prerequisite
1. Java 1.8 is needed.
 

#### Compile

1. Open a terminal and change folder to the repository path.

2. Use the following command to compile the project.

 `gradlew distZip`

3. Unzip the zip archive located in build/distributions/TTLRemover-1.0.zip in a path [PATH]

4. You can use the application in with scripts in [PATH]/bin


#### Remove TTL and create new SSTable

1. Change change folder to [PATH]/bin and the command for running the tool is as the following:

 `./TTLRemover [full path to the sstable folder] -p <output path>`

 Note: your output path must end with `\`. 
 Then, all the ttl-removed sstable is located in the `tools/bin/<output path>`.
 
 
2. To do it on batch, you can use the following command:

 `find [full path to the sstable folder]/*Data.db -type f | xargs -I PATH ./TTLRemover PATH -p <output path>`
 
3. Or you can use the command  with no guaranty 
 `./TTLRemoverKeyspace [keyspace path] -p <output path>`
 
Note: Add VM parameters : 
```
-Dcassandra-foreground=yes -Dcassandra.config=file:///C:/SECHERON/apache-cassandra-2.2.8/conf/cassandra.yaml -Dlogback.configurationFile=file:///C:/SECHERON/apache-cassandra-2.2.8/conf/logback.xml -Dcassandra-rackdc.properties=file:///C:/SECHERON/apache-cassandra-2.2.8/conf/cassandra-rackdc.properties -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=7199 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -ea -Xmx1G
```

#### Load ttl-removed SSTable to a new cluster

1. Create the keyspace and table of the target sstable in the new cluster.


2. In the source cluster, use the following command to load the ttl-removed SSTable into the new cluster.

 `./sstableloader -d <ip address of new cluster node> [path to the ttl-removed sstable folder]`
 
 







