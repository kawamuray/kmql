package kmql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;

import lombok.RequiredArgsConstructor;

public class Database implements AutoCloseable {
    @RequiredArgsConstructor
    private static class TableMetadata {
        private final Table table;
        private boolean initialized;
    }

    private final Map<String, TableMetadata> tables;
    private final Connection connection;

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

    public Database(Connection connection, TableRegistry registry) {
        this.connection = connection;
        tables = new HashMap<>();
        for (Entry<String, Table> entry : registry) {
            tables.put(entry.getKey(), new TableMetadata(entry.getValue()));
        }
    }

    public void prepareTable(String name, AdminClient adminClient) throws Exception {
        TableMetadata meta = getTable(name);
        if (meta.initialized) {
            return;
        }
        meta.table.prepare(connection, adminClient);
        meta.initialized = true;
    }

    public void prepareAllTables(AdminClient adminClient) throws Exception {
        for (String table : tables.keySet()) {
            prepareTable(table, adminClient);
        }
    }

    public void dropTable(String name) throws SQLException {
        TableMetadata meta = getTable(name);
        if (!meta.initialized) {
            throw new IllegalStateException("table not initialized: " + name);
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format("DROP TABLE \"%s\"", meta.table.name()));
        }
        meta.initialized = false;
    }

    public void executeQuery(String sql, Consumer<ResultSet> resultHandler) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet results = stmt.executeQuery(sql)) {
            resultHandler.accept(results);
        }
    }

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
