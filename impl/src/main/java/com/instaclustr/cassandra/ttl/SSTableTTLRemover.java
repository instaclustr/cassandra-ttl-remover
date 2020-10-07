package com.instaclustr.cassandra.ttl;

import java.nio.file.Path;
import java.util.Collection;

public interface SSTableTTLRemover {

    void executeRemoval(final Path outputFolder, final Collection<Path> sstables, final String cql) throws Exception;
}
