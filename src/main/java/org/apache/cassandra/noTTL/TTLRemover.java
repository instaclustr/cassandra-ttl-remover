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
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Do batch TTL removing on table
 */
public class TTLRemover {
    private static CommandLine cmd;
    private static Options options = new Options();
    private static final String OUTPUT_PATH = "p";

    static
    {
        Option outputPath = new Option(OUTPUT_PATH, true, "Output directory");
        options.addOption(outputPath);
        Util.initDatabaseDescriptor();
        Schema.instance.loadFromDisk(false);
    }


    private static void stream(Descriptor descriptor, Descriptor toSSTable) throws IOException {

        SSTableReader noTTLreader = SSTableReader.open(descriptor);

        ISSTableScanner noTTLscanner = noTTLreader.getScanner();

        ColumnFamilyStore columnFamily = ColumnFamilyStore.getIfExists(descriptor.ksname, descriptor.cfname);

        long keyCount = countKeys(descriptor, columnFamily.metadata);

        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);

        SerializationHeader header = SerializationHeader.makeWithoutStats(columnFamily.metadata);

        SSTableRewriter writer = SSTableRewriter.constructKeepingOriginals(txn, true, Long.MAX_VALUE, true);
        writer.switchWriter(SSTableWriter.create(toSSTable.toString(), keyCount, -1, header, txn));
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
                PartitionUpdate u = new PartitionUpdate(columnFamily.metadata, partition.partitionKey(), partition.columns(), 2);
                RangeTombstone.Bound start = null;
                RangeTombstone.Bound end = null;

                while(partition.hasNext()) {
                    Unfiltered unfiltered = partition.next();
                    switch(unfiltered.kind()){
                        case ROW:
                            Row newRow = serializeRow(unfiltered, columnFamily.metadata);
                            u.add(newRow);
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

    private static Row serializeRow(Unfiltered atoms, CFMetaData metaData) {

        Row row = (Row) atoms;

        ArrayList<Cell> celllist = new ArrayList<>();

        for(Cell cell: row.cells())
        {
            if (cell.isExpiring())
            {
                celllist.add(BufferCell.live(metaData, cell.column(), cell.timestamp(), cell.value()));
            }
            else {
                celllist.add(cell);
            }
        }

        return BTreeRow.create(row.clustering(), LivenessInfo.create(metaData, row.primaryKeyLivenessInfo().timestamp(), FBUtilities.nowInSeconds()), row.deletion(), celllist.toArray());
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

        if (Schema.instance.getKSMetaData(descriptor.ksname) == null)
        {
            System.err.println(String.format("Filename %s references to nonexistent keyspace: %s!",fromSSTable, descriptor.ksname));
            System.exit(1);
        }

        try
        {
            if(cmd.hasOption(OUTPUT_PATH))
            {
                String outputFolder = cmd.getOptionValue(OUTPUT_PATH);
                String toSSTableDir = outputFolder + File.separator + descriptor.ksname + File.separator + descriptor.cfname;
                File directory = new File(toSSTableDir);
                Descriptor resultDesc = new Descriptor(directory, descriptor.ksname, descriptor.cfname, descriptor.generation, SSTableFormat.Type.BIG);
                stream(descriptor, resultDesc);
            }
            else
            {
                printUsage();
                System.exit(1);
            }
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            e.printStackTrace();
            System.err.println("ERROR: " + e.getMessage());
            System.exit(-1);
        }
    }

    private static void printUsage()
    {
        System.out.printf("Usage: %s <target sstable> -p <output path>",TTLRemover.class.getName());
    }

}
