package com.instaclustr.cassandra.ttl;

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
