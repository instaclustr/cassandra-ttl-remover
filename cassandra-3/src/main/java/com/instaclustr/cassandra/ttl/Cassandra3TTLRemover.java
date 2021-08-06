package com.instaclustr.cassandra.ttl;

import static java.lang.String.format;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.instaclustr.cassandra.ttl.cli.TTLRemovalException;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.rows.Row.Builder;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cassandra3TTLRemover implements SSTableTTLRemover {

    private static final Logger logger = LoggerFactory.getLogger(Cassandra3TTLRemover.class);

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

            CFStatement parsed = (CFStatement) QueryProcessor.parseStatement(cql);
            parsed.prepareKeyspace(descriptor.ksname);
            CreateTableStatement statement = (CreateTableStatement) ((CreateTableStatement.RawStatement) parsed).prepare(Types.none()).statement;

            CFMetaData cfMetadata = statement.metadataBuilder()
                .withId(CFMetaData.generateLegacyCfId(descriptor.ksname, statement.columnFamily()))
                .withPartitioner(new Murmur3Partitioner())
                .build()
                .params(statement.params())
                .readRepairChance(0.0)
                .dcLocalReadRepairChance(0.0)
                .gcGraceSeconds(0)
                .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1));

            stream(descriptor, resultDesc, cfMetadata);
        }
    }

    public void stream(final Descriptor descriptor, final Descriptor toSSTable, final CFMetaData cfMetadata) throws TTLRemovalException {

        final SSTableReader noTTLreader;

        try {
            noTTLreader = SSTableReader.open(descriptor, SSTable.componentsFor(descriptor), cfMetadata, true, true);
        } catch (final Exception ex) {
            throw new TTLRemovalException(format("Unable to open descriptor %s", descriptor.baseFilename()), ex);
        }

        final long keyCount = countKeys(descriptor, cfMetadata);

        final LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);

        final SerializationHeader header = SerializationHeader.make(cfMetadata, Arrays.asList(noTTLreader));

        final SSTableRewriter writer = SSTableRewriter.constructKeepingOriginals(txn, true, Long.MAX_VALUE);

        writer.switchWriter(SSTableWriter.create(cfMetadata, toSSTable, keyCount, -1, 0, header, null, txn));

        //writer.switchWriter(SSTableWriter.create(toSSTable.toString(), keyCount, -1, header, null, txn));

        try (final ISSTableScanner noTTLscanner = noTTLreader.getScanner()) {
            while (noTTLscanner.hasNext()) {
                final UnfilteredRowIterator partition = noTTLscanner.next();

                if (!partition.hasNext()) {
                    //keep partitions with no rows
                    writer.append(partition);
                    continue;
                }

                final PartitionUpdate update = new PartitionUpdate(cfMetadata, partition.partitionKey(), partition.columns(), 2);

                while (partition.hasNext()) {

                    final Unfiltered unfiltered = partition.next();

                    switch (unfiltered.kind()) {
                        case ROW:
                            Row newRow = serializeRow(unfiltered);
                            update.add(newRow);
                            break;
                        case RANGE_TOMBSTONE_MARKER:
                            //Range tombstones are denoted as separate (Unfiltered) entries for start and end,
                            //so we record them separately and add the tombstone once both ends of the range are defined
                            RangeTombstoneBoundMarker marker = (RangeTombstoneBoundMarker) unfiltered;

                            final ClusteringBound start = marker.isOpen(false) ? marker.openBound(false) : null;
                            final ClusteringBound end = marker.isClose(false) ? marker.closeBound(false) : null;

                            if (start != null && end != null) {
                                update.add(new RangeTombstone(Slice.make(start, end), marker.deletionTime()));
                            }

                            break;
                    }
                }

                update.allowNewUpdates();
                writer.append(update.unfilteredIterator());
            }
            writer.finish();
        } catch (final Exception ex) {
            throw new TTLRemovalException(format("Exception occurred while scanning SSTable %s", descriptor.baseFilename()), ex);
        }
    }

    private long countKeys(final Descriptor descriptor, final CFMetaData metaData) {

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

        Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(row.clustering());

        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(row.primaryKeyLivenessInfo().timestamp(),
                                                              LivenessInfo.NO_TTL,
                                                              FBUtilities.nowInSeconds()));

        row.columnData().forEach(cd -> {
            ColumnDefinition cdef = cd.column();
            if (cdef.isComplex()) {
                Iterator<Cell> cellIterator = row.getComplexColumnData(cdef).iterator();

                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    builder.addCell(BufferCell.live(cell.column(), cell.timestamp(), cell.value(), cell.path()));
                }
            } else {
                Cell cell = row.getCell(cdef);
                builder.addCell(BufferCell.live(cell.column(), cell.timestamp(), cell.value()));
            }
        });

        builder.addRowDeletion(row.deletion());

        return builder.build();
    }
}
