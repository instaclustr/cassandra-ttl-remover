# TTLRemover
SSTable TTLRemover


### Usage

This tool is implemented based on Cassandra 2.2.

#### Prerequisite
1. Java 1.8 is needed.
 

#### Compile

1. Open a terminal and change folder to the repository path.

2. Use the following command to compile the project.

        ./gradlew jar

#### Remove TTL and create new SSTable

1. From base directory of repo run the following. This will create the resulting SSTable in the destination directory with the same generation number.

        CASSANDRA_INCLUDE=<path_to_cassandra.in.sh> ./TTLRemover <path_to_SSTable_Data.db> -p <destination_directory>
 
2. To do it on a batch of SSTables, you can use the following command:

        find [full path to the sstable folder]/*Data.db -type f | xargs -I PATH ./TTLRemover PATH -p <output path>
 
3. Or you can use the command to work on an entire keyspace. 

        ./TTLRemoverKeyspace [keyspace path] -p <output path>
 

#### Load ttl-removed SSTable to a new cluster

1. Create the keyspace and table of the target sstable in the new cluster.

2. In the source cluster, use the following command to load the ttl-removed SSTable into the new cluster.

        ./sstableloader -d <ip address of new cluster node> [path to the ttl-removed sstable folder]
 
 







