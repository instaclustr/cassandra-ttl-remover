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

import com.google.common.collect.Lists;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.service.ActiveRepairService;

import org.apache.cassandra.tools.*;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;

/**
 * Do batch TTL removing on table
 */
public class TTLRemoverKeyspace {
    private static CommandLine cmd;
    private static Options options = new Options();;
    private static final String OUTPUT_PATH = "p";
    private static final String TMP_OUTPUT_PATH = "tmp";

    static
    {
        Option outputPath = new Option(OUTPUT_PATH, true, "Output path, end with '/'");
        options.addOption(outputPath);
    }


    private static void stream(Descriptor descriptor, Descriptor toSSTable) throws IOException {
        long keyCount = countKeys(descriptor);

        NoTTLReader noTTLreader = NoTTLReader.open(descriptor);
        ISSTableScanner noTTLscanner = noTTLreader.getScanner();

        ColumnFamily columnFamily = ArrayBackedSortedColumns.factory.create(descriptor.ksname, descriptor.cfname);

        NoTTLSSTableIdentityIterator row;

        try (
                SSTableWriter writer = SSTableWriter.create(toSSTable, keyCount, ActiveRepairService.UNREPAIRED_SSTABLE,0)
        ){
            while (noTTLscanner.hasNext()) //read data from disk //NoTTLBigTableScanner
            {
                row = (NoTTLSSTableIdentityIterator) noTTLscanner.next();
                serializeRow(row,columnFamily, row.getColumnFamily().metadata());
                writer.append(row.getKey(), columnFamily);
                columnFamily.clear();
            }
        }
        finally {
            noTTLscanner.close();
        }

    }

    private static void serializeRow(Iterator<OnDiskAtom> atoms, ColumnFamily columnFamily, CFMetaData metadata) {

        while (atoms.hasNext())
        {
            serializeAtom(atoms.next(), metadata, columnFamily);
        }

    }

    private static void serializeAtom(OnDiskAtom atom, CFMetaData metadata, ColumnFamily columnFamily) {
        if (atom instanceof Cell)
        {
            Cell cell = (Cell) atom;
            if (cell instanceof BufferExpiringCell)
            {
                columnFamily.addColumn(cell.name(),cell.value(),cell.timestamp());
            }
            else if (cell instanceof BufferDeletedCell)
            {
                columnFamily.addColumn(cell);
            }
            else
                columnFamily.addColumn(cell);

        }
    }

    private static long countKeys(Descriptor descriptor) {
        KeyIterator iter = new KeyIterator(descriptor);
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
        System.out.println("Key count: "+keycount);
        return keycount;
    }

    public static void main(String[] args) throws ConfigurationException, IOException {
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

        String outputFolder = "";
        if(cmd.hasOption(OUTPUT_PATH))
        {
            outputFolder = cmd.getOptionValue(OUTPUT_PATH);
        }
        else {
            printUsage();
            System.exit(1);
        }

        Path keyspacePath = Paths.get(cmd.getArgs()[0]).toAbsolutePath();
        List<Path> sSTables = null;
        try (
                Stream<Path> stream = Files.walk(keyspacePath);
        ) {
            sSTables = stream.filter(f -> f.toString().endsWith("Data.db")).collect(Collectors.toList());
        }
        if (isNull(sSTables)) {
            System.err.println("keyspacePath " + cmd.getArgs()[0] + " is not a folder with ");
        }
        sSTables = Lists.reverse(sSTables);

        Util.initDatabaseDescriptor();
        Config.setClientMode(true);

        Schema.instance.loadFromDisk(false);  //load kspace "systemcf" and its tables;
        Keyspace.setInitialized();
        for (Path sSTable : sSTables) {
            Descriptor descriptor = Descriptor.fromFilename(sSTable.toAbsolutePath().toFile().getAbsolutePath());
            if (Schema.instance.getKSMetaData(descriptor.ksname) == null)  {
                System.err.println(String.format("Filename %s references to nonexistent keyspace: %s!",sSTable, descriptor.ksname));
                continue;
            }

            System.out.println(String.format("Loading file %s from initial keyspace: %s",sSTable, descriptor.ksname));

            try
            {
                String toSSTableDir = outputFolder + File.separator + descriptor.ksname + File.separator + descriptor.cfname;
                File directory = new File(toSSTableDir);
                directory.mkdirs();
                Descriptor resultDesc = new Descriptor(directory, descriptor.ksname, descriptor.cfname, descriptor.generation, Descriptor.Type.FINAL);
                stream(descriptor, resultDesc);
            }
            catch (Throwable e) {
                JVMStabilityInspector.inspectThrowable(e);
                e.printStackTrace();
                System.err.println("ERROR: " + e.getMessage());
            }
        }
    }

    private static void printUsage()
    {
        System.out.printf("Usage: %s <target keyspace> -p <output path>",TTLRemoverKeyspace.class.getName());
    }

}
