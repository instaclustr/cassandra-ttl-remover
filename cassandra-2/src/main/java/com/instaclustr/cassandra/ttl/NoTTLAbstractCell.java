package com.instaclustr.cassandra.ttl;

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
