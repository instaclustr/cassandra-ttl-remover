package com.instaclustr.cassandra.ttl.cli;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import com.instaclustr.cassandra.ttl.SSTableTTLRemover;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "keyspace",
    mixinStandardHelpOptions = true,
    description = "command for removing TTL from SSTables belonging to a keyspace",
    sortOptions = false,
    versionProvider = CLIApplication.class)
public class KeyspaceTTLRemoverCLI extends TTLRemoverCLI {

    @Option(names = {"--keyspace", "-ks"},
        paramLabel = "[DIRECTORY]",
        required = true,
        description = "Path to keyspace for which all SSTables will have TTL removed.")
    public Path keyspace;

    public static void main(String[] args) {
        main(args, true);
    }

    public static void main(String[] args, boolean exit) {
        main(args, new KeyspaceTTLRemoverCLI(), exit);
    }

    @Override
    public void run() {
        super.run();

        try (final Stream<Path> stream = Files.walk(keyspace)) {
            final SSTableTTLRemover ttlRemover = getTTLRemover();
            ttlRemover.executeRemoval(destination, stream.filter(f -> f.toString().endsWith("Data.db")).collect(toList()));
        } catch (final TTLRemovalException ex) {
            throw new RuntimeException(format("Unable to remove TTL from keyspace directory %s", keyspace), ex);
        } catch (final Exception ex) {
            throw new RuntimeException(format("Unable to walk keyspace directory %s", keyspace), ex);
        }
    }
}
