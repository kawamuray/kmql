package kmql;

import java.io.BufferedOutputStream;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.completer.StringsCompleter;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "kmql", mixinStandardHelpOptions = true,
        description = "SQL interface to Kafka cluster metadata")
public class Kmql implements Callable<Integer> {
    @Option(names = "--bootstrap-servers", paramLabel = "BOOTSTRAP_SERVERS",
            description = "Kafka cluster's bootstrap.servers")
    private String bootstrapServers;

    @Option(names = "--client-properties", paramLabel = "PATH",
            description = "Path to properties that contains extra properties to pass for AdminClient")
    private Path clientProperties;

    @Option(names = "--format", defaultValue = "table", paramLabel = "table|json",
            description = "Output format")
    private String outputFormat;

    @Option(names = { "-e", "--exec" }, paramLabel = "COMMAND",
            description = "Instead of starting interactive console, execute the given SQL and output the result")
    private String executeSql;

    @Option(names = "--init-all",
            description = "Initialize all tables at startup rather than lazy loading when required")
    private boolean initAllTables;

    @Override
    public Integer call() throws Exception {
        Properties adminClientConfig = adminClientConfig();
        try (AdminClient adminClient = AdminClient.create(adminClientConfig);
             Engine engine = Engine.from(adminClient, outputFormat);
             BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(FileDescriptor.out))) {
            if (initAllTables) {
                engine.initAllTables();
            }
            if (executeSql != null) {
                engine.execute(executeSql, output);
            } else {
                LineReader reader = LineReaderBuilder.builder()
                                                     .option(LineReader.Option.CASE_INSENSITIVE, true)
                                                     .completer(queryCompleter(engine.db()))
                                                     .build();
                while (true) {
                    final String sql;
                    try {
                        sql = reader.readLine("query> ").trim();
                    } catch (EndOfFileException ignored) {
                        break;
                    }
                    if (sql.isEmpty()) {
                        continue;
                    }
                    try {
                        engine.execute(sql, output);
                    } catch (SQLException e) {
                        reader.getTerminal().writer().println("Query error: " + e.getMessage());
                    }
                }
            }
        }
        return 0;
    }

    private Properties adminClientConfig() throws IOException {
        Properties props = new Properties();
        if (clientProperties != null) {
            try (FileInputStream in = new FileInputStream(clientProperties.toFile())) {
                props.load(in);
            }
        }
        if (bootstrapServers != null) {
            props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        if (props.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
            System.err.println("Missing " + AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
            CommandLine.usage(this, System.err);
            System.exit(1);
        }
        return props;
    }

    private static Completer queryCompleter(Database db) {
        Set<String> candidates = new HashSet<>();
        db.tables().forEach(name -> {
            candidates.add(name.toLowerCase());

            db.columns(name)
              .stream()
              .map(String::toLowerCase)
              .forEach(candidates::add);
        });

        return new StringsCompleter(candidates);
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Kmql()).execute(args);
        System.exit(exitCode);
    }
}
