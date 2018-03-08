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

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.serializers.MarshalException;

/**
 * Created by Linda on 8/05/2016.
 */
public class NoTTLSSTableIdentityIterator implements Comparable<NoTTLSSTableIdentityIterator>, OnDiskAtomIterator
{
    private final DecoratedKey key;
    private final DataInput in;
    public final ColumnSerializer.Flag flag;

    private final ColumnFamily columnFamily;
    private final Iterator<OnDiskAtom> atomIterator;
    private final boolean validateColumns;
    private final String filename;

    private final NoTTLReader sstable;

    public NoTTLSSTableIdentityIterator(NoTTLReader sstable, RandomAccessReader file, DecoratedKey key)
    {
        this(sstable, file, key, false);
    }

    public NoTTLSSTableIdentityIterator(NoTTLReader sstable, RandomAccessReader file, DecoratedKey key, boolean checkData)
    {
        this(sstable.metadata, file, file.getPath(), key, checkData, sstable, ColumnSerializer.Flag.LOCAL);
    }


    private NoTTLSSTableIdentityIterator(CFMetaData metadata,
                                    FileDataInput in,
                                    String filename,
                                    DecoratedKey key,
                                    boolean checkData, //false
                                    NoTTLReader sstable,
                                    ColumnSerializer.Flag flag) //local
    {
        this(metadata, in, filename, key, checkData, sstable, flag, readDeletionTime(in, sstable, filename),
             NoTTLAbstractCell.onDiskIterator(in, flag, (int) (System.currentTimeMillis() / 1000),
                                              sstable == null ? DatabaseDescriptor.getSSTableFormat().info.getLatestVersion() : sstable.descriptor.version, metadata.comparator));
    }

    private NoTTLSSTableIdentityIterator(CFMetaData metadata,
                                    DataInput in,
                                    String filename,
                                    DecoratedKey key,
                                    boolean checkData,
                                    NoTTLReader sstable,
                                    ColumnSerializer.Flag flag,
                                    DeletionTime deletion,
                                    Iterator<OnDiskAtom> atomIterator)
    {
        assert !checkData || (sstable != null);
        this.in = in;
        this.filename = filename;
        this.key = key;
        this.flag = flag;
        this.validateColumns = checkData;
        this.sstable = sstable;
        columnFamily = ArrayBackedSortedColumns.factory.create(metadata);
        columnFamily.delete(deletion);
        this.atomIterator = atomIterator;
    }

    private static DeletionTime readDeletionTime(DataInput in, NoTTLReader sstable, String filename)
    {
        try
        {
            return DeletionTime.serializer.deserialize(in);
        }
        catch (IOException e)
        {
            if (sstable != null)
                sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
    }


    public ColumnFamily getColumnFamily()
    {
        return columnFamily;
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public void close() throws IOException
    {

    }

    public boolean hasNext()
    {
        try
        {
            return atomIterator.hasNext();
        }
        catch (IOError e)
        {
            // catch here b/c atomIterator is an AbstractIterator; hasNext reads the value
            if (e.getCause() instanceof IOException)
            {
                if (sstable != null)
                    sstable.markSuspect();
                throw new CorruptSSTableException((IOException)e.getCause(), filename);
            }
            else
            {
                throw e;
            }
        }
    }

    public OnDiskAtom next()
    {
        try
        {
            OnDiskAtom atom = atomIterator.next();
            if (validateColumns)
                atom.validateFields(columnFamily.metadata());
            return atom;
        }
        catch (MarshalException me)
        {
            throw new CorruptSSTableException(me, filename);
        }
    }

    public int compareTo(NoTTLSSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }
}
