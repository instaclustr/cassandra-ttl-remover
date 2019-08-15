/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.noTTL;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Do batch TTL removing on table
 */
public class TTLRemover {
    private static CommandLine cmd;
    private static Options options = new Options();
    private static final String OUTPUT_PATH = "p";
    private static final String REMOVE_COLUMNS  = "r";
    static
    {
        Option outputPath = new Option(OUTPUT_PATH, true, "Output directory");
        Option columnsToRemove = new Option(REMOVE_COLUMNS, true, "Names of columns to remove");
        columnsToRemove.setArgs(999);
        options.addOption(outputPath);
        options.addOption(columnsToRemove);
        //Util.initDatabaseDescriptor();
        //Schema.instance.loadFromDisk(false);
        DatabaseDescriptor.toolInitialization();
    }


    private static void stream(Descriptor descriptor, Descriptor toSSTable, List<String> toRemove) throws IOException {


        CFMetaData cfm = metadataFromSSTableMinusColumns(descriptor, toRemove);

        SSTableReader noTTLreader = SSTableReader.open(descriptor, cfm);

        ISSTableScanner noTTLscanner = noTTLreader.getScanner(ColumnFilter.selection(cfm.partitionColumns()), DataRange.allData(cfm.partitioner), false
        , SSTableReadsListener.NOOP_LISTENER);

        long keyCount = countKeys(descriptor, cfm);

        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);

        SerializationHeader header = SerializationHeader.makeWithoutStats(cfm);



        SSTableRewriter writer = SSTableRewriter.constructKeepingOriginals(txn, true, Long.MAX_VALUE);
        writer.switchWriter(SSTableWriter.create(cfm,toSSTable, keyCount, noTTLreader.getSSTableMetadata().repairedAt, noTTLreader.getSSTableLevel(), header, null, txn));
        UnfilteredRowIterator partition;

        try
        {
            while (noTTLscanner.hasNext()) //read data from disk
            {
                partition = noTTLscanner.next();



                if(!partition.hasNext()){
                    //keep partitions with no rows
                    writer.append(partition);
                    continue;
                }
                PartitionUpdate u = new PartitionUpdate(cfm, partition.partitionKey(), cfm.partitionColumns(), 2);
                ClusteringBound start = null;
                ClusteringBound end = null;

                while(partition.hasNext()) {
                    Unfiltered unfiltered = partition.next();
                    switch(unfiltered.kind()){
                        case ROW:
                            //Row newRow = serializeRow(unfiltered, cfm);
                            u.add((Row)unfiltered);
                            break;
                        case RANGE_TOMBSTONE_MARKER:
                            //Range tombstones are denoted as separate (Unfiltered) entries for start and end,
                            //so we record them separately and add the tombstone once both ends of the range are defined
                            RangeTombstoneBoundMarker marker = (RangeTombstoneBoundMarker) unfiltered;

                            if(marker.isOpen(false)){
                                start = marker.openBound(false);
                            }
                            if(marker.isClose(false)){
                                end = marker.closeBound(false);
                            }
                            if(start != null && end != null){
                                u.add(new RangeTombstone(Slice.make(start, end), marker.deletionTime()));
                                start = null;
                                end = null;
                            }
                            break;
                    }
                }

                writer.append(u.unfilteredIterator());
            }
            writer.finish();
        }
        finally
        {
            noTTLscanner.close();
        }

    }


    private static long countKeys(Descriptor descriptor, CFMetaData metaData) {
        KeyIterator iter = new KeyIterator(descriptor, metaData);
        long keycount = 0;
        try
        {
            while (iter.hasNext())
            {
                iter.next();
                keycount++;
            }

        }
        finally
        {
            iter.close();
        }
        return keycount;
    }

    public static void main(String[] args) throws ConfigurationException
    {
        CommandLineParser parser = new PosixParser();

        try
        {
            cmd = parser.parse(options,args);
        }
        catch (ParseException e)
        {
            System.err.println(e.getMessage());
            printUsage();
            System.exit(1);
        }

        if (cmd.getArgs().length != 1)
        {
            printUsage();
            System.exit(1);
        }

        String fromSSTable = new File(cmd.getArgs()[0]).getAbsolutePath();

        Descriptor descriptor = Descriptor.fromFilename(fromSSTable);


        try
        {
            if(cmd.hasOption(OUTPUT_PATH))
            {
                String outputFolder = cmd.getOptionValue(OUTPUT_PATH);
                String toSSTableDir = outputFolder + File.separator + descriptor.ksname + File.separator + descriptor.cfname;
                File directory = new File(toSSTableDir);
                if(!directory.exists()) {
                    directory.mkdirs();
                }
                Descriptor resultDesc = new Descriptor(directory, descriptor.ksname, descriptor.cfname, descriptor.generation, SSTableFormat.Type.BIG);
                if (cmd.hasOption(REMOVE_COLUMNS))
                    stream(descriptor, resultDesc, Arrays.asList(cmd.getOptionValues(REMOVE_COLUMNS)));
                else
                    stream(descriptor, resultDesc, (List<String>)(Collections.EMPTY_LIST));
            }
            else
            {
                printUsage();
                System.exit(1);
            }
        }
        catch (Exception e)
        {

            e.printStackTrace();
            System.err.println("ERROR: " + e.getMessage());
            System.exit(-1);
        }
    }

    private static void printUsage()
    {
        System.out.printf("Usage: %s <target sstable> -p <output path>",TTLRemover.class.getName());
    }


    /**
     * Construct table schema from info stored in SSTable's Stats.db
     *
     * @param desc SSTable's descriptor
     * @return Restored CFMetaData
     * @throws IOException when Stats.db cannot be read
     */
    public static CFMetaData metadataFromSSTableMinusColumns(Descriptor desc, List<String> toRemoveNames) throws IOException
    {
        if (!desc.version.storeRows())
            throw new IOException("pre-3.0 SSTable is not supported.");

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        IPartitioner partitioner = FBUtilities.newPartitioner(desc);

        CFMetaData.Builder builder = CFMetaData.Builder.create("keyspace", "table").withPartitioner(partitioner);
        header.getStaticColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addStaticColumn(ident, entry.getValue());
                });
        header.getRegularColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addRegularColumn(ident, entry.getValue());
                });
        builder.addPartitionKey("PartitionKey", header.getKeyType());
        for (int i = 0; i < header.getClusteringTypes().size(); i++)
        {
            builder.addClusteringColumn("clustering" + (i > 0 ? i : ""), header.getClusteringTypes().get(i));
        }

        CFMetaData cfm = builder.build();
        for (String name : toRemoveNames)
        {
            try {
                ColumnDefinition toRemove = cfm.getColumnDefinition(ColumnIdentifier.getInterned(name, true));
                cfm.removeColumnDefinition(toRemove);
                cfm.recordColumnDrop(toRemove, 1);
            }
            catch (Exception e)
            {
                System.out.printf("Column %s not found in sstable %s\n", name, desc.filenameFor(Component.DATA));
            }
        }

        return cfm;
    }
}
