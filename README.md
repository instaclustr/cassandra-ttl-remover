# TTLRemover
SSTable TTLRemover


### Usage

This tool is implemented for Cassandra version 2.2.

#### Prerequisite
1. Download source code of Cassandra 2.2 using followng command:

`git clone -b cassandra-2.2 git://git.apache.org/cassandra.git`

2. Link `noTTL` folder into the `src/java/org/apache/cassandra/` folder of cassandra using following command:

`ln -s <abusolute path to the noTTL folder> <abusolute path to src/java/org/apache/cassandra/ folder >`

Note: you must use abusolute path in this command, otherwise some errors would be involed. 

3. Link `TTLRemover` bash script into `tool/bin` folder of cassandra using the following cammand:

`ln -s <abusolute path to the TTLRemover file> <abusolute path to tool/bin/ folder>`

#### Compile

1. Open a terminal and change folder to the cassandra root folder

2. Use the following command to comiple the project

`ant generate-idea-files`


#### Remove TTL and create new SSTable

1. Change change foler to cassandra/tool/bin and the command for running the tool is as the following:

`./TTLRemover [full path to the sstable folder>] -p <output path>`

Note: your output path must end with `\`. Then, all the ttl-removed sstable is located in the tools/bin/<output path>


2. To do it on bactch, you can use the folloing command:

`find [full path to the sstable folder]/*Data.db -type f | xargs -I PATH ./TTLRemover PATH -p <output path>`

#### Load ttl-removed SSTable to a new cluster

1. Create the keyspace and table of the target sstable in the new cluster.


2. In the source cluster, use the following command to load the ttl-removed SSTable into the new cluster.

`./sstableloader -d <ip address of new cluster node> [path to the ttl-removed sstable folder]`







