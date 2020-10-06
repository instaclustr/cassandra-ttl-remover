package com.instaclustr.cassandra.ttl.cli;

import static java.util.stream.Collectors.toList;

import java.nio.file.Path;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import com.instaclustr.cassandra.ttl.SSTableTTLRemover;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

public abstract class TTLRemoverCLI implements Runnable {

    @Spec
    protected CommandSpec spec;

    @Option(names = {"--output-path", "-p"},
        paramLabel = "[DIRECTORY]",
        required = true,
        description = "Destination where SSTable will be generated.")
    protected Path destination;

    @Option(names = {"--cassandra-yaml", "-f"},
        description = "Path to cassandra.yaml file for loading generated SSTables to Cassandra",
        required = true)
    public Path cassandraYaml;

    @Option(names = {"--cassandra-storage-dir", "-c"},
        description = "Path to cassandra data dir",
        required = true)
    public Path cassandraStorageDir;

    public static void main(String[] args, TTLRemoverCLI ttlRemoverCLI, boolean exit) {

        int exitCode = CLIApplication.execute(ttlRemoverCLI, args);

        if (exit) {
            System.exit(exitCode);
        }
    }

    @Override
    public void run() {
        JarManifestVersionProvider.logCommandVersionInformation(spec);

        if (!Boolean.parseBoolean(System.getProperty("ttl.remover.tests", "false"))) {
            TTLRemoverCLI.setProperties(cassandraYaml, cassandraStorageDir);
        }
    }

    public static void setProperties(final Path cassandraYaml, final Path cassandraStorageDir) {
        System.setProperty("cassandra.config", "file://" + cassandraYaml.toAbsolutePath().toString());
        System.setProperty("cassandra.storagedir", cassandraStorageDir.toAbsolutePath().toString());
    }

    public static SSTableTTLRemover getTTLRemover() throws TTLRemovalException {
        final ServiceLoader<SSTableTTLRemover> serviceLoader = ServiceLoader.load(SSTableTTLRemover.class);

        final List<SSTableTTLRemover> removers = StreamSupport.stream(serviceLoader.spliterator(), false).collect(toList());

        if (removers.size() == 0) {
            throw new TTLRemovalException("Unable to locate an instance of SSTableTTLRemover on the class path.");
        } else if (removers.size() != 1) {
            throw new TTLRemovalException(String.format("There is %s implementations of %s on the class path, there needs to be just one!",
                                                        removers.size(),
                                                        SSTableTTLRemover.class.getName()));
        }

        return removers.get(0);
    }
}
