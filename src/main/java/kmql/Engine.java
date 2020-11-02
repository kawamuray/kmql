package kmql;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.clients.admin.AdminClient;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

/**
 * A core runtime of kmql.
 */
@AllArgsConstructor
@Accessors(fluent = true)
@Getter
public class Engine implements AutoCloseable {
    private final AdminClient adminClient;
    private final Database db;
    private final OutputFormatRegistry outputFormatRegistry;
    private final CommandRegistry commandRegistry;
    @NonNull
    private OutputFormat outputFormat;

    /**
     * Creates a new {@link Engine} from the given {@link AdminClient} and the name of the output format.
     * Default instances are used for both of {@link OutputFormatRegistry} and {@link TableRegistry}.
     * @param adminClient an {@link AdminClient} to access Kafka cluster's metadata.
     * @param outputFormatName the name of output format to use.
     * @return an {@link Engine}.
     */
    public static Engine from(AdminClient adminClient, String outputFormatName) {
        OutputFormat outputFormat = lookupOutputFormat(OutputFormatRegistry.DEFAULT, outputFormatName);
        Database db = Database.from(TableRegistry.DEFAULT);
        return new Engine(adminClient, db, OutputFormatRegistry.DEFAULT, CommandRegistry.DEFAULT, outputFormat);
    }

    /**
     * Execute the given command.
     * Command should be a valid SQL for now.
     * @param command command to execute.
     * @param output the output stream to write the formatted query result.
     * @throws SQLException when SQL fails.
     */
    public void execute(String command, BufferedOutputStream output) throws SQLException {
        if (Commands.isCommand(command)) {
            Commands.executeLine(this, output, command);
            return;
        }

        prepareRequiredTables(command);
        db.executeQuery(command, results -> {
            try {
                outputFormat.formatTo(results, output);
                output.flush();
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void prepareRequiredTables(String sql) {
        List<String> requiredTables = SqlAnalyzer.requiredTables(sql);
        for (String table : requiredTables) {
            try {
                db.prepareTable(table, adminClient);
            } catch (IllegalArgumentException ignored) {
                // skip failed tables for now...
            } catch (Exception e) {
                throw new RuntimeException("Error initializing table: " + table, e);
            }
        }
    }

    /**
     * Initialize all tables that this engine supports.
     */
    public void initAllTables() {
        try {
            db.prepareAllTables(adminClient);
        } catch (Exception e) {
            throw new RuntimeException("failed to initialize tables", e);
        }
    }

    /**
     * Set the output format to the given instance of {@link OutputFormat}.
     * @param outputFormat a new output format to use.
     */
    public void setOutputFormat(@NonNull OutputFormat outputFormat) {
        this.outputFormat = outputFormat;
    }

    /**
     * Set the output format to the specified instance by the given name.
     * @param name name of the output format.
     */
    public void setOutputFormat(String name) {
        OutputFormat newFormat = lookupOutputFormat(outputFormatRegistry, name);
        setOutputFormat(newFormat);
    }

    private static OutputFormat lookupOutputFormat(OutputFormatRegistry registry, String name) {
        return registry.lookup(name)
                       .orElseThrow(() -> new IllegalArgumentException("unknown output format: " + name));
    }

    @Override
    public void close() throws Exception {
        db.close();
    }
}
