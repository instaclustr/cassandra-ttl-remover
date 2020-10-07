package com.instaclustr.cassandra.ttl.cli;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.instaclustr.cassandra.ttl.SSTableTTLRemover;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

@Command(name = "ttl-remove",
    mixinStandardHelpOptions = true,
    description = "command for removing TTL from SSTables",
    sortOptions = false,
    versionProvider = TTLRemoverCLI.class)
public class TTLRemoverCLI extends JarManifestVersionProvider implements Runnable {

    @Spec
    protected CommandSpec spec;

    @Option(names = {"--output-path", "-p"},
        paramLabel = "[DIRECTORY]",
        required = true,
        description = "Destination where SSTable will be generated.")
    protected Path destination;

    @Option(names = {"--cassandra-yaml", "-f"},
        paramLabel = "[FILE]",
        description = "Path to cassandra.yaml file for loading generated SSTables to Cassandra, relevant only in case of Cassandra 2.")
    public Path cassandraYaml;

    @Option(names = {"--cassandra-storage-dir", "-d"},
        paramLabel = "[DIRECTORY]",
        description = "Path to cassandra data dir, relevant only in case of Cassandra 2.")
    public Path cassandraStorageDir;

    @Option(names = {"--sstables", "-s"},
        paramLabel = "[DIRECTORY]",
        description = "Path to a directory for which all SSTables will have TTL removed.")
    public Path sstables;

    @Option(names = {"--sstable", "-t"},
        paramLabel = "[FILE]",
        description = "Path to .db file of a SSTable which will have TTL removed.")
    public Path sstable;

    @Option(names = {"--cassandra-version", "-c"},
        paramLabel = "[INTEGER]",
        converter = CassandraVersionConverter.class,
        description = "Version of Cassandra to remove TTL for, might be 2, 3 or 4, defaults to 3")
    public CassandraVersion cassandraVersion;

    @Option(names = {"--cql", "-q"},
        paramLabel = "[CQL]",
        description = "CQL statement which creates table we want to remove TTL from. This has to be set in case --cassandra-version is 3 or 4")
    public String cql;

    public static void main(String[] args) {
        main(args, true);
    }

    public static void main(String[] args, boolean exit) {
        int exitCode = execute(new CommandLine(new TTLRemoverCLI()), args);

        if (exit) {
            System.exit(exitCode);
        }
    }

    @Override
    public String getImplementationTitle() {
        return "ttl-remove";
    }

    @Override
    public void run() {
        JarManifestVersionProvider.logCommandVersionInformation(spec);

        validate();

        if (!Boolean.parseBoolean(System.getProperty("ttl.remover.tests", "false"))) {
            TTLRemoverCLI.setProperties(cassandraYaml, cassandraStorageDir, cassandraVersion);
        }

        try {
            final SSTableTTLRemover ttlRemover = getTTLRemover();
            ttlRemover.executeRemoval(destination, getSSTables(), cql);
        } catch (final Exception ex) {
            throw new RuntimeException("Unable to remove TTLs from SSTables", ex);
        }
    }

    public static int execute(CommandLine commandLine, String... args) {
        return commandLine
            .setErr(new PrintWriter(System.err))
            .setOut(new PrintWriter(System.err))
            .setUnmatchedArgumentsAllowed(false)
            .setColorScheme(new CommandLine.Help.ColorScheme.Builder().ansi(CommandLine.Help.Ansi.ON).build())
            .setExecutionExceptionHandler((ex, cmdLine, parseResult) -> {

                ex.printStackTrace();

                return 1;
            })
            .execute(args);
    }


    // relevant for Cassandra 2 only
    public static void setProperties(final Path cassandraYaml, final Path cassandraStorageDir, final CassandraVersion version) {
        if (version == CassandraVersion.V2) {
            System.setProperty("cassandra.config", "file://" + cassandraYaml.toAbsolutePath().toString());
            System.setProperty("cassandra.storagedir", cassandraStorageDir.toAbsolutePath().toString());
        }
    }

    private SSTableTTLRemover getTTLRemover() throws TTLRemovalException {
        final ServiceLoader<SSTableTTLRemover> serviceLoader = ServiceLoader.load(SSTableTTLRemover.class);

        final List<SSTableTTLRemover> removers = StreamSupport.stream(serviceLoader.spliterator(), false).collect(toList());

        if (removers.size() == 0) {
            throw new TTLRemovalException("Unable to locate an instance of SSTableTTLRemover on the class path.");
        } else if (removers.size() != 1) {
            throw new TTLRemovalException(format("There is %s implementations of %s on the class path, there needs to be just one!",
                                                 removers.size(),
                                                 SSTableTTLRemover.class.getName()));
        }

        return removers.get(0);
    }

    private Collection<Path> getSSTables() throws TTLRemovalException {
        if (sstables != null) {
            try (final Stream<Path> stream = Files.walk(sstables)) {
                return stream.filter(f -> f.toString().endsWith("Data.db")).collect(toList());
            } catch (final Exception ex) {
                throw new RuntimeException(format("Unable to walk keyspace directory %s", sstables), ex);
            }
        } else if (sstable != null) {
            if (!sstable.toFile().exists() || sstable.toFile().canRead()) {
                throw new RuntimeException(format("SSTable %s does not exist or it can not be read.", sstable));
            }

            return Collections.singleton(sstable);
        }

        throw new TTLRemovalException("--sstables nor --sstable parameter was set, you have to set one of them!");
    }

    private void validate() {
        if (cassandraVersion != CassandraVersion.V2 && cql == null) {
            throw new ParameterException(spec.commandLine(),
                                         format("You want to remove TTL from SSTables for version %s but you have not specified --cql",
                                                cassandraVersion));
        }

        if (cassandraVersion == CassandraVersion.V2) {
            if (cassandraYaml == null) {
                throw new ParameterException(spec.commandLine(), "You set Cassandra version to '2' but you have not set --cassandra-yaml");
            }
            if (cassandraStorageDir == null) {
                throw new ParameterException(spec.commandLine(), "You set Cassandra version to '2' but you have not set --cassandra-storage-dir");
            }
        }

        if (cassandraVersion != CassandraVersion.V2) {
            if (cassandraYaml != null) {
                throw new ParameterException(spec.commandLine(), format("You set Cassandra version to '%s' but you have set --cassandra-yaml", cassandraVersion));
            }
            if (cassandraStorageDir != null) {
                throw new ParameterException(spec.commandLine(), format("You set Cassandra version to '%s' but you have set --cassandra-storage-dir", cassandraVersion));
            }
        }

        if (sstables == null && sstable == null) {
            throw new ParameterException(spec.commandLine(), "You have not specified --sstables nor --sstable.");
        }

        if (sstables != null && sstable != null) {
            throw new ParameterException(spec.commandLine(), "You have specified both --sstables and --sstable.");
        }
    }

    private static final class CassandraVersionConverter implements ITypeConverter<CassandraVersion> {

        @Override
        public CassandraVersion convert(final String value) {
            return CassandraVersion.parse(value);
        }
    }

    public enum CassandraVersion {
        V2("2"),
        V3("3"),
        V4("4");

        final String version;

        CassandraVersion(final String version) {
            this.version = version;
        }

        public static CassandraVersion parse(final String version) {
            if (version == null) {
                return V3;
            }

            if (version.equals("2")) {
                return V2;
            }

            if (version.equals("3")) {
                return V3;
            }

            if (version.equals("4")) {
                return V4;
            }

            return V3;
        }
    }
}