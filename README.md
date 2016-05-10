# TTLRemover
SSTable TTLRemover


### Usage

This tool is implemented for Cassandra version 2.2.

#### Prerequisite
1. Download source code of TTLRemover to `TTLRemover` folder using following command:

 `git clone https://github.com/instaclustr/TTLRemover.git TTLRemover`


2. Download source code of Cassandra 2.2 to `cassandra-2.2-src` folder using followng command:

 `git clone -b cassandra-2.2 git://git.apache.org/cassandra.git cassandra-2.2-src`


3. Link `noTTL` folder into the `src/java/org/apache/cassandra/` folder of cassandra using following command:

 `ln -s <absolute path to the noTTL folder> <absolute path to src/java/org/apache/cassandra/ folder>`


4. Link `TTLRemover` bash script into `tool/bin` folder of cassandra using the following command:

 `ln -s <absolute path to the TTLRemover file> <absolute path to tool/bin/ folder>`
 
 Note: you must use absolute path in this command, otherwise some errors would be involved. 

#### Compile

1. Open a terminal and change folder to the cassandra root folder.

2. Use the following command to compile the project.

 `ant generate-idea-files`


#### Remove TTL and create new SSTable

1. Change change foler to cassandra/tool/bin and the command for running the tool is as the following:

 `./TTLRemover [full path to the sstable folder>] -p <output path>`

 Note: your output path must end with `\`. Then, all the ttl-removed sstable is located in the tools/bin/<output path>.


2. To do it on batch, you can use the following command:

 `find [full path to the sstable folder]/*Data.db -type f | xargs -I PATH ./TTLRemover PATH -p <output path>`
 

#### Load ttl-removed SSTable to a new cluster

1. Create the keyspace and table of the target sstable in the new cluster.


2. In the source cluster, use the following command to load the ttl-removed SSTable into the new cluster.

 `./sstableloader -d <ip address of new cluster node> [path to the ttl-removed sstable folder]`
 
 







