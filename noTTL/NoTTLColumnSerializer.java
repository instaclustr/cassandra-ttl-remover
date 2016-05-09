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
import java.nio.ByteBuffer;

import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.BufferCounterCell;
import org.apache.cassandra.db.BufferCounterUpdateCell;
import org.apache.cassandra.db.BufferDeletedCell;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Created by Linda on 3/05/2016.
 */
public class NoTTLColumnSerializer extends ColumnSerializer
{
    public NoTTLColumnSerializer(CellNameType type)
    {
        super(type);
    }

    Cell deserializeColumnBody(DataInput in, CellName name, int mask, Flag flag, int expireBefore) throws IOException
    {
        if ((mask & COUNTER_MASK) != 0)
        {
            long timestampOfLastDelete = in.readLong();
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            return BufferCounterCell.create(name, value, ts, timestampOfLastDelete, flag);
        }
        else if ((mask & EXPIRATION_MASK) != 0)
        {
            int ttl = in.readInt();
            int expiration = in.readInt();
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            return new BufferCell(name, value, ts);
        }
        else
        {
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            return (mask & COUNTER_UPDATE_MASK) != 0
                   ? new BufferCounterUpdateCell(name, value, ts)
                   : ((mask & DELETION_MASK) == 0
                      ? new BufferCell(name, value, ts)
                      : new BufferDeletedCell(name, value, ts));
        }
    }

}
