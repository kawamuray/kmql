package kmql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;

import lombok.RequiredArgsConstructor;

/**
 * A database representation that stores metadata for the Kafka cluster.
 */
public class Database implements AutoCloseable {
    @RequiredArgsConstructor
    private static class TableMetadata {
        private final Table table;
        private boolean initialized;
    }

    private final Map<String, TableMetadata> tables;
    private final Connection connection;

    /**
     * Create a new {@link Database} that supports the tables in the given {@link TableRegistry}.
     * @param registry registry containing tables to support.
     * @return a {@link Database}.
     */
    public static Database from(TableRegistry registry) {
        Connection connection = createConnection();
        return new Database(connection, registry);
    }

    private static Connection createConnection() {
        try {
            Class.forName("org.h2.Driver");
            return DriverManager.getConnection("jdbc:h2:mem:");
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    Database(Connection connection, TableRegistry registry) {
        this.connection = connection;
        tables = new HashMap<>();
        for (Entry<String, Table> entry : registry) {
            tables.put(entry.getKey(), new TableMetadata(entry.getValue()));
        }
    }

    /**
     * Prepare the given table if it hasn'et yet initialized.
     * @param name the name of the table.
     * @param adminClient an {@link AdminClient} to access Kafka cluster metadata.
     * @throws Exception when SQL failed or {@link AdminClient} threw.
     */
    public void prepareTable(String name, AdminClient adminClient) throws Exception {
        TableMetadata meta = getTable(name);
        if (meta.initialized) {
            return;
        }
        Collection<String> dependencyTables = meta.table.dependencyTables();
        for (String dependencyTable : dependencyTables) {
            prepareTable(dependencyTable, adminClient);
        }
        meta.table.prepare(connection, adminClient);
        meta.initialized = true;
    }

    /**
     * Prepare all tables that this database supports.
     * @param adminClient an {@link AdminClient} to access Kafka cluster metadata.
     * @throws Exception when SQL failed or {@link AdminClient} threw.
     */
    public void prepareAllTables(AdminClient adminClient) throws Exception {
        for (String table : tables.keySet()) {
            prepareTable(table, adminClient);
        }
    }

    /**
     * Drop the table of the given name.
     * @param name the name of the table.
     * @throws SQLException when SQL failed.
     */
    public void dropTable(String name) throws SQLException {
        TableMetadata meta = getTable(name);
        if (!meta.initialized) {
            throw new IllegalStateException("table not initialized: " + name);
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format("DROP TABLE %s", meta.table.name()));
        }
        meta.initialized = false;
    }

    /**
     * Drop all tables.
     * @throws SQLException when SQL failed.
     */
    public void dropAllTables() throws SQLException {
        for (Entry<String, TableMetadata> entry : tables.entrySet()) {
            String name = entry.getKey();
            TableMetadata meta = entry.getValue();
            if (meta.initialized) {
                dropTable(name);
            }
        }
    }

    /**
     * Execute the given query and call the given handler with the {@link ResultSet}.
     * @param sql an SQL query.
     * @param resultHandler callback handler that processes the the result.
     * @throws SQLException when SQL failed.
     */
    public void executeQuery(String sql, Consumer<ResultSet> resultHandler) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet results = stmt.executeQuery(sql)) {
            resultHandler.accept(results);
        }
    }

    /**
     * Return if the given table has initialized.
     * @param name the name of the table.
     * @return true if the table has initialized.
     */
    public boolean tableInitialized(String name) {
        return getTable(name).initialized;
    }

    private TableMetadata getTable(String name) {
        TableMetadata meta = tables.get(name);
        if (meta == null) {
            throw new IllegalArgumentException("no such table: " + name);
        }
        return meta;
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
