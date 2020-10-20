package kmql;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.clients.admin.AdminClient;

import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
public class Engine implements AutoCloseable {
    private final AdminClient adminClient;
    private final Database db;
    private final OutputFormatRegistry outputFormatRegistry;
    @NonNull
    private OutputFormat outputFormat;

    public static Engine from(AdminClient adminClient, String outputFormatName) {
        OutputFormatRegistry outputFormatRegistry = OutputFormatRegistry.DEFAULT;
        OutputFormat outputFormat = lookupOutputFormat(outputFormatRegistry, outputFormatName);
        Database db = Database.from(TableRegistry.DEFAULT);
        return new Engine(adminClient, db, outputFormatRegistry, outputFormat);
    }

    public void execute(String command, BufferedOutputStream output) throws SQLException {
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

    public void initAllTables() {
        try {
            db.prepareAllTables(adminClient);
        } catch (Exception e) {
            throw new RuntimeException("failed to initialize tables", e);
        }
    }

    public void setOutputFormat(@NonNull OutputFormat outputFormat) {
        this.outputFormat = outputFormat;
    }

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
