package com.instaclustr.cassandra.ttl;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.io.sstable.format.Version;

public class NoTTLSerializer extends OnDiskAtom.Serializer {

    private final CellNameType type;

    public NoTTLSerializer(CellNameType type) {
        super(type);
        this.type = type;
    }

    @Override
    public OnDiskAtom deserializeFromSSTable(DataInput in, ColumnSerializer.Flag flag, int expireBefore, Version version) throws IOException {
        Composite name = type.serializer().deserialize(in);
        if (name.isEmpty()) {
            // SSTableWriter.END_OF_ROW
            return null;
        }

        int b = in.readUnsignedByte();
        if ((b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0) {
            return type.rangeTombstoneSerializer().deserializeBody(in, name, version);
        } else {
            return new NoTTLColumnSerializer(type).deserializeColumnBody(in, (CellName) name, b, flag, expireBefore);
        }
    }

}
