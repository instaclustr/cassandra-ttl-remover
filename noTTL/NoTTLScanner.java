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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.dht.AbstractBounds.isEmpty;
import static org.apache.cassandra.dht.AbstractBounds.maxLeft;
import static org.apache.cassandra.dht.AbstractBounds.minRight;

/**
 * Created by Linda on 8/05/2016.
 *
 *
 * Cannot extend BigTableScanner because its constructor is private.
 */
public class NoTTLScanner implements ISSTableScanner
{
    private AtomicBoolean isClosed = new AtomicBoolean(false);
    protected final RandomAccessReader dfile;
    protected final RandomAccessReader ifile;
    public final NoTTLReader sstable;

    private final Iterator<AbstractBounds<RowPosition>> rangeIterator;
    private AbstractBounds<RowPosition> currentRange;

    private final DataRange dataRange;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    protected Iterator<OnDiskAtomIterator> iterator;

    public static ISSTableScanner getScanner(NoTTLReader sstable, DataRange dataRange, RateLimiter limiter)
    {
        System.out.println("\n\nhello here it is\n\n");
        return new NoTTLScanner(sstable, dataRange, limiter);
    }


    private NoTTLScanner(NoTTLReader sstable, DataRange dataRange, RateLimiter limiter)
    {
        assert sstable != null;

        this.dfile = limiter == null ? sstable.openDataReader() : sstable.openDataReader(limiter);
        this.ifile = sstable.openIndexReader();
        this.sstable = sstable;
        this.dataRange = dataRange;
        this.rowIndexEntrySerializer = sstable.descriptor.version.getSSTableFormat().getIndexSerializer(sstable.metadata);

        List<AbstractBounds<RowPosition>> boundsList = new ArrayList<>(2);
        addRange(dataRange.keyRange(), boundsList);
        this.rangeIterator = boundsList.iterator();
    }

    private void addRange(AbstractBounds<RowPosition> requested, List<AbstractBounds<RowPosition>> boundsList)
    {
        if (requested instanceof Range && ((Range)requested).isWrapAround())
        {
            if (requested.right.compareTo(sstable.first) >= 0)
            {
                // since we wrap, we must contain the whole sstable prior to stopKey()
                AbstractBounds.Boundary<RowPosition> left = new AbstractBounds.Boundary<RowPosition>(sstable.first, true);
                AbstractBounds.Boundary<RowPosition> right;
                right = requested.rightBoundary();
                right = minRight(right, sstable.last, true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
            if (requested.left.compareTo(sstable.last) <= 0)
            {
                // since we wrap, we must contain the whole sstable after dataRange.startKey()
                AbstractBounds.Boundary<RowPosition> right = new AbstractBounds.Boundary<RowPosition>(sstable.last, true);
                AbstractBounds.Boundary<RowPosition> left;
                left = requested.leftBoundary();
                left = maxLeft(left, sstable.first, true);
                if (!isEmpty(left, right))
                    boundsList.add(AbstractBounds.bounds(left, right));
            }
        }
        else
        {
            assert requested.left.compareTo(requested.right) <= 0 || requested.right.isMinimum();
            AbstractBounds.Boundary<RowPosition> left, right;
            left = requested.leftBoundary();
            right = requested.rightBoundary();
            left = maxLeft(left, sstable.first, true);
            // apparently isWrapAround() doesn't count Bounds that extend to the limit (min) as wrapping
            right = requested.right.isMinimum() ? new AbstractBounds.Boundary<RowPosition>(sstable.last, true)
                                                : minRight(right, sstable.last, true);
            if (!isEmpty(left, right))
                boundsList.add(AbstractBounds.bounds(left, right));
        }
    }

    public void close() throws IOException
    {
        if (isClosed.compareAndSet(false, true))
            FileUtils.close(dfile, ifile);
    }

    public long getLengthInBytes()
    {
        return dfile.length();
    }

    public long getCurrentPosition()
    {
        return dfile.getFilePointer();
    }

    public String getBackingFiles()
    {
        return sstable.toString();
    }

    public boolean hasNext()
    {
        if (iterator == null)
            iterator = createIterator();
        return iterator.hasNext();
    }

    public OnDiskAtomIterator next()
    {
        if (iterator == null)  // iterator is not null
            iterator = createIterator();
        return iterator.next();
    }

    private Iterator<OnDiskAtomIterator> createIterator()
    {
        return new NoTTLKeyScanningIterator();
    }

    private void seekToCurrentRangeStart()
    {
        long indexPosition = sstable.getIndexScanPosition(currentRange.left);
        ifile.seek(indexPosition);
        try
        {

            while (!ifile.isEOF())
            {
                indexPosition = ifile.getFilePointer();
                DecoratedKey indexDecoratedKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                if (indexDecoratedKey.compareTo(currentRange.left) > 0 || currentRange.contains(indexDecoratedKey))
                {
                    // Found, just read the dataPosition and seek into index and data files
                    long dataPosition = ifile.readLong();
                    ifile.seek(indexPosition);
                    dfile.seek(dataPosition);
                    break;
                }
                else
                {
                    RowIndexEntry.Serializer.skip(ifile);
                }
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    protected class NoTTLKeyScanningIterator extends AbstractIterator<OnDiskAtomIterator>
    {
        private DecoratedKey nextKey;
        private RowIndexEntry nextEntry;
        private DecoratedKey currentKey;
        private RowIndexEntry currentEntry;

        protected OnDiskAtomIterator computeNext()
        {
            try
            {
                if (nextEntry == null)
                {
                    do
                    {
                        // we're starting the first range or we just passed the end of the previous range
                        if (!rangeIterator.hasNext())
                            return endOfData();

                        currentRange = rangeIterator.next();
                        seekToCurrentRangeStart();

                        if (ifile.isEOF())
                            return endOfData();

                        currentKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                        currentEntry = rowIndexEntrySerializer.deserialize(ifile, sstable.descriptor.version);
                    } while (!currentRange.contains(currentKey));
                }
                else
                {
                    // we're in the middle of a range
                    currentKey = nextKey;
                    currentEntry = nextEntry;
                }

                if (ifile.isEOF())
                {
                    nextEntry = null;
                    nextKey = null;
                }
                else
                {
                    // we need the position of the start of the next key, regardless of whether it falls in the current range
                    nextKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                    nextEntry = rowIndexEntrySerializer.deserialize(ifile, sstable.descriptor.version);

                    if (!currentRange.contains(nextKey))
                    {
                        nextKey = null;
                        nextEntry = null;
                    }
                }


                dfile.seek(currentEntry.position + currentEntry.headerOffset());
                ByteBufferUtil.readWithShortLength(dfile); // key
                return new NoTTLSSTableIdentityIterator(sstable, dfile, currentKey,false);


            }
            catch (CorruptSSTableException | IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.getFilename());
            }
        }
    }

}
