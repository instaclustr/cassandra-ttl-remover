# TTLRemover
SSTable TTLRemover


### Usage

This tool is implemented for Cassandra version 2.2.

#### Prerequisite

1.Put TTLRemover.java into a src/java/org/apache/cassandra/tool folder, and put TTLRemover bash script into tool/bin folder.


2.In BufferExpiringCell.java, change `return new BufferDeletedCell` to `return BufferExpiringCell`.


#### Remove TTL and create new SSTable

1. Change change foler to cassandra/tool/bin and the command for running the tool is as the following:

`./TTLRemover [full path to the sstable folder>] -p <output path>`

Then, all the ttl-removed sstable is located in the tools/bin/<output path>


2. To do it on bactch, you can use the folloing command:

`find [full path to the sstable folder]/*Data.db -type f | xargs -I PATH ./TTLRemover PATH -p <output path>`

#### Load ttl-removed SSTable to a new cluster

1. Create the keyspace and table of the target sstable in the new cluster.


2. In the source cluster, use the following command to load the ttl-removed SSTable into the new cluster.

`./sstableloader -d <ip address of new cluster node> [path to the ttl-removed sstable folder]`







