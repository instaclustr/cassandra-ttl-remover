package com.instaclustr.cassandra.ttl.cli;

import static java.lang.String.format;

import java.nio.file.Path;
import java.util.Collections;

import com.instaclustr.cassandra.ttl.SSTableTTLRemover;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "sstable",
    mixinStandardHelpOptions = true,
    description = "command for removing TTL from a SSTable",
    sortOptions = false,
    versionProvider = CLIApplication.class)
public class SSTableTTLRemoverCLI extends TTLRemoverCLI {

    @Option(names = {"--sstable", "-ss"},
        paramLabel = "[FILE]",
        required = true,
        description = "Path to .db file of a SSTable")
    private Path sstable;

    public static void main(String[] args) {
        main(args, true);
    }

    public static void main(String[] args, boolean exit) {
        main(args, new SSTableTTLRemoverCLI(), exit);
    }

    @Override
    public void run() {
        super.run();

        try {
            if (!sstable.toFile().exists() || sstable.toFile().canRead()) {
                throw new RuntimeException(format("SSTable %s does not exist or it can not be read.", sstable));
            }
            final SSTableTTLRemover ttlRemover = getTTLRemover();
            ttlRemover.executeRemoval(destination, Collections.singleton(sstable));
        } catch (final TTLRemovalException ex) {
            throw new RuntimeException(format("Unable to remove TTL from SSTable %s", sstable), ex);
        }
    }
}
