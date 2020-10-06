package com.instaclustr.cassandra.ttl;

import static java.lang.String.format;

import java.nio.file.Path;
import java.util.Collection;

import com.instaclustr.cassandra.ttl.cli.TTLRemovalException;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Row.Builder;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cassandra4TTLRemover implements SSTableTTLRemover {

    private static final Logger logger = LoggerFactory.getLogger(Cassandra4TTLRemover.class);

    @Override
    public void executeRemoval(final Path outputFolder, final Collection<Path> sstables) throws TTLRemovalException {

        if (!DatabaseDescriptor.isToolInitialized()) {
            DatabaseDescriptor.toolInitialization(false);
        }

        Util.initDatabaseDescriptor();
        Config.setClientMode(true);
        try {
            Schema.instance.loadFromDisk(false);
        } catch (final Exception ex) {

        }
        Keyspace.setInitialized();

        try {
            for (final Path sstable : sstables) {
                final Descriptor descriptor = Descriptor.fromFilename(sstable.toAbsolutePath().toFile().getAbsolutePath());

                if (Schema.instance.getKeyspaceMetadata(descriptor.ksname) == null) {
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
                                                             SSTableFormat.Type.BIG);
                stream(descriptor, resultDesc);
            }
        } catch (final Exception ex) {
            throw new TTLRemovalException("Unable to remove TTL from SSTable-s.", ex);
        }
    }

    public void stream(final Descriptor descriptor, final Descriptor toSSTable) throws TTLRemovalException {

        final SSTableReader noTTLreader;

        try {
            noTTLreader = SSTableReader.open(descriptor);
        } catch (final Exception ex) {
            throw new TTLRemovalException(format("Unable to open descriptor %s", descriptor.baseFilename()), ex);
        }

        final ColumnFamilyStore columnFamily = ColumnFamilyStore.getIfExists(descriptor.ksname, descriptor.cfname);

        final TableMetadata tableMetadata = columnFamily.metadata.get();

        final long keyCount = countKeys(descriptor, tableMetadata);

        final LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);

        final SerializationHeader header = SerializationHeader.makeWithoutStats(tableMetadata);

        final SSTableRewriter writer = SSTableRewriter.constructKeepingOriginals(txn, true, Long.MAX_VALUE);

        writer.switchWriter(SSTableWriter.create(toSSTable, keyCount, -1, null, false, header, null, txn));

        try (final ISSTableScanner noTTLscanner = noTTLreader.getScanner()) {
            while (noTTLscanner.hasNext()) {
                final UnfilteredRowIterator partition = noTTLscanner.next();

                if (!partition.hasNext()) {
                    //keep partitions with no rows
                    writer.append(partition);
                    continue;
                }

                final PartitionUpdate.Builder builder = new PartitionUpdate.Builder(columnFamily.metadata.get(),
                                                                                    partition.partitionKey(),
                                                                                    partition.columns(),
                                                                                    2,
                                                                                    false);

                while (partition.hasNext()) {

                    final Unfiltered unfiltered = partition.next();

                    switch (unfiltered.kind()) {
                        case ROW:
                            Row newRow = serializeRow(unfiltered);
                            builder.add(newRow);
                            break;
                        case RANGE_TOMBSTONE_MARKER:
                            //Range tombstones are denoted as separate (Unfiltered) entries for start and end,
                            //so we record them separately and add the tombstone once both ends of the range are defined
                            RangeTombstoneBoundMarker marker = (RangeTombstoneBoundMarker) unfiltered;

                            final ClusteringBound start = marker.isOpen(false) ? marker.openBound(false) : null;
                            final ClusteringBound end = marker.isClose(false) ? marker.closeBound(false) : null;

                            if (start != null && end != null) {
                                builder.add(new RangeTombstone(Slice.make(start, end), marker.deletionTime()));
                            }

                            break;
                    }
                }

                writer.append(builder.build().unfilteredIterator());
            }
            writer.finish();
        } catch (final Exception ex) {
            throw new TTLRemovalException(format("Exception occured while scanning SSTable %s", descriptor.baseFilename()), ex);
        }
    }

    private long countKeys(final Descriptor descriptor, final TableMetadata metaData) {

        final KeyIterator iter = new KeyIterator(descriptor, metaData);

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

    private Row serializeRow(final Unfiltered atoms) {

        final Row row = (Row) atoms;

        Builder builder = BTreeRow.unsortedBuilder();

        for (final Cell cell : row.cells()) {
            builder.addCell(BufferCell.live(cell.column(), cell.timestamp(), cell.value()));
        }

        builder.newRow(row.clustering());
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(row.primaryKeyLivenessInfo().timestamp(),
                                                              LivenessInfo.NO_TTL,
                                                              FBUtilities.nowInSeconds()));
        builder.addRowDeletion(row.deletion());

        return builder.build();
    }
}
