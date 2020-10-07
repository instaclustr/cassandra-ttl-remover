package com.instaclustr.cassandra.ttl;

import static java.lang.String.format;

import java.nio.file.Path;
import java.util.Collection;

import com.instaclustr.cassandra.ttl.cli.TTLRemovalException;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ClusteringBound;
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
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cassandra4TTLRemover implements SSTableTTLRemover {

    private static final Logger logger = LoggerFactory.getLogger(Cassandra4TTLRemover.class);

    @Override
    public void executeRemoval(final Path outputFolder, final Collection<Path> sstables, final String cql) throws Exception {

        for (final Path sstable : sstables) {
            final Descriptor descriptor = Descriptor.fromFilename(sstable.toAbsolutePath().toFile().getAbsolutePath());

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

            final TableMetadata tableMetadata = CreateTableStatement.parse(cql, descriptor.ksname).partitioner(new Murmur3Partitioner()).build();

            stream(descriptor, resultDesc, tableMetadata);
        }
    }

    public void stream(final Descriptor descriptor, final Descriptor toSSTable, final TableMetadata tableMetadata) throws TTLRemovalException {

        final SSTableReader sourceSSTableReader;

        try {
            sourceSSTableReader = SSTableReader.open(descriptor, SSTable.componentsFor(descriptor), TableMetadataRef.forOfflineTools(tableMetadata), true, true);
            //sourceSSTableReader = SSTableReader.open(descriptor, TableMetadataRef.forOfflineTools(tableMetadata));
        } catch (final Exception ex) {
            throw new TTLRemovalException(format("Unable to open descriptor %s", descriptor.baseFilename()), ex);
        }

        final long keyCount = countKeys(descriptor, tableMetadata);

        final LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);

        final SerializationHeader header = SerializationHeader.makeWithoutStats(tableMetadata);

        final SSTableRewriter writer = SSTableRewriter.constructKeepingOriginals(txn, true, Long.MAX_VALUE);

        writer.switchWriter(SSTableWriter.create(TableMetadataRef.forOfflineTools(tableMetadata), toSSTable, keyCount, -1, null, false, 0, header, null, txn));

        try (final ISSTableScanner sourceSSTableScanner = sourceSSTableReader.getScanner()) {
            while (sourceSSTableScanner.hasNext()) {
                final UnfilteredRowIterator partition = sourceSSTableScanner.next();

                if (!partition.hasNext()) {
                    //keep partitions with no rows
                    writer.append(partition);
                    continue;
                }

                final PartitionUpdate.Builder builder = new PartitionUpdate.Builder(tableMetadata,
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
            throw new TTLRemovalException(format("Exception occurred while scanning SSTable %s", descriptor.baseFilename()), ex);
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
