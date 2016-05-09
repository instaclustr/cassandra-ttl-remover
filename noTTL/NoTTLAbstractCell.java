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

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.AbstractCell;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.io.sstable.format.Version;

/**
 * Created by Linda on 3/05/2016.
 */
public abstract class NoTTLAbstractCell extends AbstractCell
{
    public static Iterator<OnDiskAtom> onDiskIterator(final DataInput in,
                                                      final ColumnSerializer.Flag flag,
                                                      final int expireBefore,
                                                      final Version version,
                                                      final CellNameType type)
    {
        return new AbstractIterator<OnDiskAtom>()
        {
            protected OnDiskAtom computeNext()
            {
                OnDiskAtom atom;
                try
                {
                    atom = new NoTTLSerializer(type).deserializeFromSSTable(in, flag, expireBefore, version);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
                if (atom == null)
                    return endOfData();

                return atom;
            }
        };
    }
}
