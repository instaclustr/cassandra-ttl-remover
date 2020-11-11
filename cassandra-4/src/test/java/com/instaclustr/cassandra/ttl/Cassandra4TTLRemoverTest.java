package com.instaclustr.cassandra.ttl;

import static java.lang.String.format;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.github.nosan.embedded.cassandra.EmbeddedCassandraFactory;
import com.github.nosan.embedded.cassandra.api.Cassandra;
import com.github.nosan.embedded.cassandra.api.Version;
import com.github.nosan.embedded.cassandra.artifact.Artifact;
import com.instaclustr.cassandra.ttl.cli.TTLRemoverCLI;
import com.instaclustr.sstable.generator.BulkLoader;
import com.instaclustr.sstable.generator.CassandraBulkLoader;
import com.instaclustr.sstable.generator.Generator;
import com.instaclustr.sstable.generator.MappedRow;
import com.instaclustr.sstable.generator.RowMapper;
import com.instaclustr.sstable.generator.SSTableGenerator;
import com.instaclustr.sstable.generator.cli.CLIApplication;
import com.instaclustr.sstable.generator.exception.SSTableGeneratorException;
import com.instaclustr.sstable.generator.specs.BulkLoaderSpec;
import com.instaclustr.sstable.generator.specs.CassandraBulkLoaderSpec;
import com.instaclustr.sstable.generator.specs.CassandraBulkLoaderSpec.CassandraVersion;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tools.Cassandra4CustomBulkLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;

@RunWith(JUnit4.class)
public class Cassandra4TTLRemoverTest {

    private static final Logger logger = LoggerFactory.getLogger(Cassandra4TTLRemoverTest.class);

    private static final String CASSANDRA_VERSION = System.getProperty("cassandra4.version", "4.0-beta3");

    private static final String KEYSPACE = "test";

    private static final String TABLE = "test";

    private static Artifact CASSANDRA_ARTIFACT = Artifact.ofVersion(Version.of(CASSANDRA_VERSION));

    @Rule
    public TemporaryFolder noTTLSSTables = new TemporaryFolder();

    @Rule
    public TemporaryFolder generatedSSTables = new TemporaryFolder();

    @Test
    public void removeTTL() throws InterruptedException {

        System.out.println(System.getProperty("java.library.path"));

        Path cassandraDir = new File("target/cassandra-4").toPath().toAbsolutePath();

        EmbeddedCassandraFactory cassandraFactory = new EmbeddedCassandraFactory();
        cassandraFactory.setWorkingDirectory(cassandraDir);
        cassandraFactory.setArtifact(CASSANDRA_ARTIFACT);
        cassandraFactory.getJvmOptions().add("-Xmx1g");
        cassandraFactory.getJvmOptions().add("-Xms1g");

        Cassandra cassandra = null;

        try {
            cassandra = cassandraFactory.create();
            cassandra.start();

            waitForCql();

            executeWithSession(session -> {
                session.execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };", KEYSPACE));
                session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id uuid, name text, surname text, PRIMARY KEY (id)) WITH default_time_to_live = 10;", KEYSPACE, TABLE));
            });

            // this has to be here for streaming in loader ... yeah, just here
            System.setProperty("cassandra.storagedir", cassandraDir.resolve("data").toAbsolutePath().toString());
            System.setProperty("cassandra.config", "file://" + findCassandraYaml(new File("target/cassandra-4/conf").toPath()).toAbsolutePath().toString());
            DatabaseDescriptor.toolInitialization(false);

            final BulkLoaderSpec bulkLoaderSpec = new BulkLoaderSpec();

            bulkLoaderSpec.bufferSize = 128;
            bulkLoaderSpec.file = Paths.get("");
            bulkLoaderSpec.keyspace = KEYSPACE;
            bulkLoaderSpec.table = TABLE;
            bulkLoaderSpec.partitioner = "murmur";
            bulkLoaderSpec.sorted = false;
            bulkLoaderSpec.threads = 1;

            bulkLoaderSpec.generationImplementation = TestFixedImplementation.class.getName();
            bulkLoaderSpec.outputDir = generatedSSTables.getRoot().toPath();
            bulkLoaderSpec.schema = Paths.get(new File("src/test/resources/cassandra/cql/table.cql").getAbsolutePath());

            final BulkLoader bulkLoader = new TestBulkLoader();
            bulkLoader.bulkLoaderSpec = bulkLoaderSpec;

            bulkLoader.run();

            // wait until data would expire
            Thread.sleep(15000);

            //
            // load data and see that we do not have them there as they expired

            final CassandraBulkLoaderSpec cassandraBulkLoaderSpec = new CassandraBulkLoaderSpec();
            cassandraBulkLoaderSpec.node = "127.0.0.1";
            cassandraBulkLoaderSpec.cassandraYaml = findCassandraYaml(new File("target/cassandra-4/conf").toPath());
            cassandraBulkLoaderSpec.sstablesDir = bulkLoaderSpec.outputDir;
            cassandraBulkLoaderSpec.cassandraVersion = CassandraVersion.V3;

            final CassandraBulkLoader cassandraBulkLoader = new Cassandra4CustomBulkLoader();
            cassandraBulkLoader.cassandraBulkLoaderSpec = cassandraBulkLoaderSpec;

            // here we see they expired

            try (final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build(); final Session session = cluster.connect()) {
                assertEquals(0, session.execute(QueryBuilder.select().all().from("test", "test")).all().size());
            }

            cassandra.stop();

            logger.info("Removing TTLs ...");

            // remove ttls

            TTLRemoverCLI.main(new String[]{
                "--cassandra-version=4",
                "--sstables",
                bulkLoaderSpec.outputDir.toAbsolutePath().toString() + "/test",
                "--output-path",
                noTTLSSTables.getRoot().toPath().toString(),
                "--cql",
                "CREATE TABLE IF NOT EXISTS test.test (id uuid, name text, surname text, PRIMARY KEY (id)) WITH default_time_to_live = 10;"
            }, false);

            // start new Cassandra instance

            cassandra = cassandraFactory.create();
            cassandra.start();

            executeWithSession(session -> {
                session.execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };", KEYSPACE));
                session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id uuid, name text, surname text, PRIMARY KEY (id)) WITH default_time_to_live = 10;", KEYSPACE, TABLE));
            });

            // import it into Cassandra

            final CassandraBulkLoader cassandraBulkLoader2 = new Cassandra4CustomBulkLoader();
            cassandraBulkLoader2.cassandraBulkLoaderSpec = cassandraBulkLoaderSpec;

            final CassandraBulkLoaderSpec cassandraBulkLoaderSpec2 = new CassandraBulkLoaderSpec();

            cassandraBulkLoaderSpec2.node = "127.0.0.1";
            cassandraBulkLoaderSpec2.cassandraYaml = findCassandraYaml(new File("target/cassandra-4/conf").toPath());
            cassandraBulkLoaderSpec2.sstablesDir = Paths.get(noTTLSSTables.getRoot().getAbsolutePath(), KEYSPACE, TABLE);
            cassandraBulkLoaderSpec2.cassandraVersion = CassandraVersion.V4;
            cassandraBulkLoaderSpec2.keyspace = KEYSPACE;

            cassandraBulkLoader2.cassandraBulkLoaderSpec = cassandraBulkLoaderSpec2;
            cassandraBulkLoader2.run();

            // but here, we have them!
            try (final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build(); final Session session = cluster.connect()) {
                List<Row> results = session.execute(QueryBuilder.select().all().from("test", "test")).all();

                results.forEach(row -> logger.info(format("id: %s, name: %s, surname: %s", row.getUUID("id"), row.getString("name"), row.getString("surname"))));

                assertEquals(3, results.size());
            }
        } finally {
            if (cassandra != null) {
                cassandra.stop();
            }
        }
    }

    public static final class TestFixedImplementation implements RowMapper {

        public static final String KEYSPACE = "test";
        public static final String TABLE = "test";

        public static final UUID UUID_1 = UUID.randomUUID();
        public static final UUID UUID_2 = UUID.randomUUID();
        public static final UUID UUID_3 = UUID.randomUUID();

        @Override
        public List<Object> map(final List<String> row) {
            return null;
        }

        @Override
        public Stream<List<Object>> get() {
            return Stream.of(
                new ArrayList<Object>() {{
                    add(UUID_1);
                    add("John");
                    add("Doe");
                }},
                new ArrayList<Object>() {{
                    add(UUID_2);
                    add("Marry");
                    add("Poppins");
                }},
                new ArrayList<Object>() {{
                    add(UUID_3);
                    add("Jim");
                    add("Jack");
                }});
        }

        @Override
        public List<Object> random() {
            return null;
        }

        @Override
        public String insertStatement() {
            return format("INSERT INTO %s.%s (id, name, surname) VALUES (?, ?, ?);", KEYSPACE, TABLE);
        }
    }


    private void waitForCql() {
        await()
            .pollInterval(10, TimeUnit.SECONDS)
            .pollInSameThread()
            .timeout(1, TimeUnit.MINUTES)
            .until(() -> {
                try (final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build()) {
                    cluster.connect();
                    return true;
                } catch (final Exception ex) {
                    return false;
                }
            });
    }

    public void executeWithSession(Consumer<Session> supplier) {
        try (final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build()) {
            try (final Session session = cluster.connect()) {
                supplier.accept(session);
            }
        }
    }

    private Path findCassandraYaml(final Path confDir) {

        try {
            return Files.list(confDir)
                .filter(path -> path.getFileName().toString().contains("-cassandra.yaml"))
                .findFirst()
                .orElseThrow(RuntimeException::new);
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to list or there is not any file ending on -cassandra.yaml" + confDir);
        }
    }

    @Command(name = "fixed",
        mixinStandardHelpOptions = true,
        description = "tool for bulk-loading of fixed data",
        sortOptions = false,
        versionProvider = CLIApplication.class)
    public static final class TestBulkLoader extends BulkLoader {

        @Override
        public Generator getLoader(final BulkLoaderSpec bulkLoaderSpec, final SSTableGenerator ssTableWriter) {
            return new TestGenerator(ssTableWriter);
        }

        private static final class TestGenerator implements Generator {

            private final SSTableGenerator ssTableGenerator;

            public TestGenerator(final SSTableGenerator ssTableGenerator) {
                this.ssTableGenerator = ssTableGenerator;
            }

            @Override
            public void generate(final RowMapper rowMapper) {
                try {
                    ssTableGenerator.generate(rowMapper.get().filter(Objects::nonNull).map(MappedRow::new).iterator());
                } catch (final Exception ex) {
                    throw new SSTableGeneratorException("Unable to generate SSTables from FixedLoader.", ex);
                }
            }
        }
    }
}
