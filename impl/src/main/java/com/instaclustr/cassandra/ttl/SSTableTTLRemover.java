package com.instaclustr.cassandra.ttl;

import java.nio.file.Path;
import java.util.Collection;

import com.instaclustr.cassandra.ttl.cli.TTLRemovalException;

public interface SSTableTTLRemover {

    void executeRemoval(final Path outputFolder, final Collection<Path> sstables) throws TTLRemovalException;
}
