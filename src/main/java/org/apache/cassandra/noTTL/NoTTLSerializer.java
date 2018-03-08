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
import java.io.IOException;

import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.io.sstable.format.Version;

/**
 * Created by Linda on 3/05/2016.
 */
public class NoTTLSerializer extends OnDiskAtom.Serializer
{
    private final CellNameType type;
    public NoTTLSerializer(CellNameType type)
    {
        super(type);
        this.type=type;
    }

    @Override
    public OnDiskAtom deserializeFromSSTable(DataInput in, ColumnSerializer.Flag flag, int expireBefore, Version version) throws IOException
    {
        Composite name = type.serializer().deserialize(in);
        if (name.isEmpty())
        {
            // SSTableWriter.END_OF_ROW
            return null;
        }

        int b = in.readUnsignedByte();
        if ((b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0)
            return type.rangeTombstoneSerializer().deserializeBody(in, name, version);
        else
            return new NoTTLColumnSerializer(type).deserializeColumnBody(in, (CellName)name, b, flag, expireBefore);
    }

}
