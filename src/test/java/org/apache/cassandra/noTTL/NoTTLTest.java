package org.apache.cassandra.noTTL;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class NoTTLTest {

    private String[] args = new String[3];

    @Before
    public void setUp() throws Exception {
        args[0] = "./src/resources/my_keyspace/timeseries/mc-2-big-Data.db"; //sample SSTable containing TTLs, tombstones, range tombstones and partition deletions
        args[1] = "-p";
        args[2] = "/tmp/nottltest";
    }

    @Test
    public void testTTLRemoval() {

        TTLRemover.main(args);  //run TTLRemover

        //Load the result SSTable, try to read it, iterate over it, validate that TTLs are removed, range tombstones are still there.
        String newpath = "/tmp/nottltest/my_keyspace/timeseries/mc-2-Data.db";

        Descriptor descriptor = Descriptor.fromFilename(newpath);

        try {
            SSTableReader noTTLreader = SSTableReader.open(descriptor);

            ISSTableScanner noTTLscanner = noTTLreader.getScanner();

            ColumnFamilyStore columnFamily = ColumnFamilyStore.getIfExists(descriptor.ksname, descriptor.cfname);

            UnfilteredRowIterator partition;

            boolean tombstoneFound = false;
            boolean rangeTombstoneFound = false;

            try
            {
                while (noTTLscanner.hasNext()) //read data from disk
                {
                    partition = noTTLscanner.next();

                    RangeTombstone.Bound start = null;
                    RangeTombstone.Bound end = null;

                    if(!partition.hasNext()){   //check that partition deletion is still there
                        assertEquals(1533270606330537L, partition.partitionLevelDeletion().markedForDeleteAt());
                    }

                    while(partition.hasNext()) {
                        Unfiltered unfiltered = partition.next();
                        switch(unfiltered.kind()){
                            case ROW:
                                Row row = (Row) unfiltered;
                                for(Cell cell: row.cells())
                                {
                                    if(!cell.isTombstone()) {
                                        if (cell.value().getInt() == 7) {
                                            assertEquals(0, cell.ttl());    //validate that a row has TTL removed
                                        }
                                    }

                                    if(cell.isTombstone()){ //validate the tombstone
                                        tombstoneFound = true;
                                        assertEquals(1533270605, cell.localDeletionTime());
                                    }
                                }
                                break;
                            case RANGE_TOMBSTONE_MARKER:
                                //Make sure the range tombstone is still in the new SSTable
                                RangeTombstoneBoundMarker marker = (RangeTombstoneBoundMarker) unfiltered;

                                if(marker.isOpen(false)){
                                    assertEquals(1533270605297319L, marker.openDeletionTime(false).markedForDeleteAt());
                                    assert(!marker.openIsInclusive(false));

                                }
                                if(marker.isClose(false)){
                                    assertEquals(1533270605297319L, marker.closeDeletionTime(false).markedForDeleteAt());
                                    assert(marker.closeIsInclusive(false));
                                }
                                rangeTombstoneFound = true;
                                break;
                        }
                    }

                }
                assert(tombstoneFound); //check the tombstone is still there
                assert(rangeTombstoneFound); //check the tombstone is still there
            }
            finally
            {
                noTTLscanner.close();
            }

        } catch (IOException e) {
            fail("Could not open SSTable.");
        }


    }
}
