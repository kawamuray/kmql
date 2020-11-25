package kmql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

    private static void createTable(Connection connection, Table table) {
        try {
            table.create(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    Database(Connection connection, TableRegistry registry) {
        this.connection = connection;
        tables = new HashMap<>();
        for (Entry<String, Table> entry : registry) {
            tables.put(entry.getKey(), new TableMetadata(entry.getValue()));
            createTable(connection, entry.getValue());
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
     * Truncate the table of the given name.
     * @param name the name of the table.
     * @throws SQLException when SQL failed.
     */
    public void truncateTable(String name) throws SQLException {
        TableMetadata meta = getTable(name);
        if (!meta.initialized) {
            throw new IllegalStateException("table not initialized: " + name);
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format("TRUNCATE TABLE %s", meta.table.name()));
        }
        meta.initialized = false;
    }

    /**
     * Truncate all tables.
     * @throws SQLException when SQL failed.
     */
    public void truncateAllTables() throws SQLException {
        for (Entry<String, TableMetadata> entry : tables.entrySet()) {
            String name = entry.getKey();
            TableMetadata meta = entry.getValue();
            if (meta.initialized) {
                truncateTable(name);
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

    /**
     * Return tables contained in this database.
     * @return the set of table names.
     */
    public Set<String> tables() {
        return tables.values().stream().map(m -> m.table.name()).collect(Collectors.toSet());
    }

    /**
     * Return columns of the given table.
     * @param name the target table name.
     * @return the list of column names.
     */
    public List<String> columns(String name) {
        ensureTablePresence(name);
        List<String> columns = new ArrayList<>();
        try {
            executeQuery("SHOW COLUMNS FROM " + name, results -> {
                try {
                    while (results.next()) {
                        columns.add(results.getString(1));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return columns;
    }

    private TableMetadata getTable(String name) {
        ensureTablePresence(name);
        return tables.get(name);
    }

    private void ensureTablePresence(String name) {
        if (!tables.containsKey(name)) {
            throw new IllegalArgumentException("no such table: " + name);
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
