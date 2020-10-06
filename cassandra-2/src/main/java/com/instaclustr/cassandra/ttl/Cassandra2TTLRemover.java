package com.instaclustr.cassandra.ttl;

import static java.lang.String.format;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;

import com.instaclustr.cassandra.ttl.cli.TTLRemovalException;
import com.instaclustr.cassandra.ttl.cli.TTLRemoverCLI;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.BufferDeletedCell;
import org.apache.cassandra.db.BufferExpiringCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.Descriptor.Type;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.service.ActiveRepairService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cassandra2TTLRemover implements SSTableTTLRemover {

    private static final Logger logger = LoggerFactory.getLogger(Cassandra2TTLRemover.class);

    @Override
    public void executeRemoval(final Path outputFolder, final Collection<Path> sstables) throws TTLRemovalException {

        if (!Boolean.parseBoolean(System.getProperty("ttl.remover.tests", "false"))) {
            DatabaseDescriptor.forceStaticInitialization();
        }

        //DatabaseDescriptor.forceStaticInitialization();

        //Config.setClientMode(true);

        Config.setClientMode(false);

        //Util.initDatabaseDescriptor();

        if (Boolean.parseBoolean(System.getProperty("ttl.remover.tests", "false"))) {
            DatabaseDescriptor.applyConfig(DatabaseDescriptor.loadConfig());
        }

        //DatabaseDescriptor.applyConfig(DatabaseDescriptor.loadConfig());

        try {
            Schema.instance.loadFromDisk(false);
        } catch (final Exception ex) {
            // important as that would fail when we are on tests
        }
        Keyspace.setInitialized();

        try {
            for (final Path sstable : sstables) {

                final Descriptor descriptor = Descriptor.fromFilename(sstable.toAbsolutePath().toFile().getAbsolutePath());

                if (Schema.instance.getKSMetaData(descriptor.ksname) == null) {
                    logger.warn(format("Filename %s references to nonexistent keyspace: %s!", sstable, descriptor.ksname));
                    continue;
                }

                logger.info(format("Loading file %s from initial keyspace: %s", sstable, descriptor.ksname));

                final Path newSSTableDestinationDir = outputFolder.resolve(descriptor.ksname).resolve(descriptor.cfname);

                if (!newSSTableDestinationDir.toFile().exists()) {
                    if (!newSSTableDestinationDir.toFile().mkdirs()) {
                        throw new TTLRemovalException(format("Unable to create directories leading to %s.", newSSTableDestinationDir.toFile().getAbsolutePath()));
                    }
                }

                final Descriptor resultDesc = new Descriptor(newSSTableDestinationDir.toFile(),
                                                             descriptor.ksname,
                                                             descriptor.cfname,
                                                             descriptor.generation,
                                                             Type.FINAL,
                                                             SSTableFormat.Type.BIG);

                stream(descriptor, resultDesc);
            }
        } catch (final Exception ex) {
            throw new TTLRemovalException("Unable to remove TTL from SSTable-s.", ex);
        }
    }

    public void stream(final Descriptor descriptor, final Descriptor toSSTable) throws TTLRemovalException {

        ISSTableScanner noTTLscanner = null;

        try {
            long keyCount = countKeys(descriptor);

            NoTTLReader noTTLreader = NoTTLReader.open(descriptor);

            noTTLscanner = noTTLreader.getScanner();

            ColumnFamily columnFamily = ArrayBackedSortedColumns.factory.create(descriptor.ksname, descriptor.cfname);

            SSTableWriter writer = SSTableWriter.create(toSSTable, keyCount, ActiveRepairService.UNREPAIRED_SSTABLE);

            NoTTLSSTableIdentityIterator row;

            while (noTTLscanner.hasNext()) //read data from disk //NoTTLBigTableScanner
            {
                row = (NoTTLSSTableIdentityIterator) noTTLscanner.next();
                serializeRow(row, columnFamily);
                writer.append(row.getKey(), columnFamily);
                columnFamily.clear();
            }

            writer.finish(true);
        } catch (final Exception ex) {
            throw new TTLRemovalException("Unable to remove TTL from sstables.", ex);
        } finally {
            if (noTTLscanner != null) {
                try {
                    noTTLscanner.close();
                } catch (final Exception ex) {
                    throw new TTLRemovalException("Unable to close TTL scanner", ex);
                }
            }
        }
    }

    private void serializeRow(Iterator<OnDiskAtom> atoms, ColumnFamily columnFamily) {

        while (atoms.hasNext()) {
            serializeAtom(atoms.next(), columnFamily);
        }

    }

    private void serializeAtom(OnDiskAtom atom, ColumnFamily columnFamily) {
        if (atom instanceof Cell) {
            Cell cell = (Cell) atom;
            if (cell instanceof BufferExpiringCell) {
                columnFamily.addColumn(cell.name(), cell.value(), cell.timestamp());
            } else if (cell instanceof BufferDeletedCell) {
                columnFamily.addColumn(cell);
            } else {
                columnFamily.addColumn(cell);
            }

        }
    }

    private long countKeys(Descriptor descriptor) {
        KeyIterator iter = new KeyIterator(descriptor);
        long keycount = 0;
        try {
            while (iter.hasNext()) {
                iter.next();
                keycount++;
            }

        } finally {
            iter.close();
        }

        return keycount;
    }
}
