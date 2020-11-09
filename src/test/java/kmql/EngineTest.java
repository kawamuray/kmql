package kmql;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.admin.AdminClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class EngineTest {
    private Engine engine;

    @Mock
    private AdminClient adminClient;
    private Connection connection;
    private final List<String> outputs = new ArrayList<>();

    @Before
    public void setUp() {
        TableRegistry tableRegistry = new TableRegistry();
        tableRegistry.register("xyz", new Table() {
            @Override
            public String name() {
                return "xyz";
            }

            @Override
            public void create(Connection connection) throws Exception {
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("CREATE TABLE xyz (id VARCHAR(255) NOT NULL)");
                }
            }

            @Override
            public void prepare(Connection connection, AdminClient adminClient) throws Exception {
                try (Statement stmt = connection.createStatement()) {
                    stmt.executeUpdate("INSERT INTO xyz VALUES ('foo'), ('bar'), ('baz')");
                }
            }
        });
        connection = SqlUtils.connection();
        Database db = new Database(connection, tableRegistry);
        OutputFormatRegistry outputFormatRegistry = new OutputFormatRegistry();
        OutputFormat rawFormat = (results, out) -> {
            while (results.next()) {
                String id = results.getString(1);
                outputs.add(id);
            }
        };
        outputFormatRegistry.register("raw", rawFormat);

        CommandRegistry commandRegistry = new CommandRegistry();
        commandRegistry.register("xyz", new Command() {
            @Override
            public String help() {
                return "";
            }

            @Override
            public void execute(List<String> args, Engine engine, BufferedOutputStream output) {
                try {
                    output.write("xyz".getBytes());
                } catch (IOException ignored) {}
            }
        });
        engine = new Engine(adminClient, db, outputFormatRegistry, commandRegistry, rawFormat);
    }

    @After
    public void tearDown() throws Exception {
        engine.close();
    }

    @Test
    public void executeSql() throws SQLException {
        // This call should initialize the table internally
        engine.execute("SELECT id FROM xyz", mock(BufferedOutputStream.class));
        assertEquals(Arrays.asList("foo", "bar", "baz"), outputs);
        outputs.clear();
        // This call should use existing table
        engine.execute("SELECT id FROM xyz", mock(BufferedOutputStream.class));
        assertEquals(Arrays.asList("foo", "bar", "baz"), outputs);
    }

    @Test
    public void executeCommand() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (BufferedOutputStream bout = new BufferedOutputStream(output)) {
            engine.execute(":xyz", bout);
        }
        assertEquals("xyz", new String(output.toByteArray()));
    }

    @Test
    public void initAllTables() throws SQLException {
        engine.initAllTables();
        SqlUtils.tableNonEmpty(connection, "xyz");
    }
}
