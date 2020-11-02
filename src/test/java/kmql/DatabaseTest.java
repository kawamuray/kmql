package kmql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.AdminClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class DatabaseTest {
    @Rule
    public final MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private Table xyzTable;
    @Mock
    private Table fooTable;
    @Mock
    private AdminClient adminClient;
    private final TableRegistry registry = new TableRegistry();
    private Connection connection;
    private Database db;

    @Before
    public void setUp() throws Exception {
        doReturn("xyz").when(xyzTable).name();
        doAnswer(invocation -> {
            Connection conn = invocation.getArgument(0);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE xyz (id INT NOT NULL)");
            }
            return null;
        }).when(xyzTable).prepare(any(Connection.class), any(AdminClient.class));
        doReturn("foo").when(fooTable).name();
        registry.register("xyz", xyzTable);
        registry.register("foo", fooTable);
        connection = SqlUtils.connection();
        db = new Database(connection, registry);
    }

    @After
    public void tearDown() throws Exception {
        db.close();
    }

    @Test
    public void prepareTable() throws Exception {
        db.prepareTable("xyz", adminClient);
        verify(xyzTable, times(1)).prepare(connection, adminClient);
        assertTrue(SqlUtils.tableExists(connection, "xyz"));
        // This should be no-op because it's already initialized
        db.prepareTable("xyz", adminClient);
        verify(xyzTable, times(1)).prepare(connection, adminClient);
    }

    @Test(expected = IllegalArgumentException.class)
    public void prepareAbsentTable() throws Exception {
        db.prepareTable("no-such-table", adminClient);
    }

    @Test
    public void prepareAllTables() throws Exception {
        db.prepareAllTables(adminClient);
        verify(xyzTable, times(1)).prepare(connection, adminClient);
        verify(fooTable, times(1)).prepare(connection, adminClient);
    }

    @Test
    public void dropTable() throws Exception {
        db.prepareTable("xyz", adminClient);
        db.dropTable("xyz");
        assertFalse(SqlUtils.tableExists(connection, "xyz"));
    }

    @Test(expected = IllegalStateException.class)
    public void dropAbsentTable() throws Exception {
        db.dropTable("xyz");
    }

    @Test
    public void dropAllTables() throws Exception {
        db.prepareTable("xyz", adminClient);
        db.dropAllTables();
        assertFalse(SqlUtils.tableExists(connection, "xyz"));
        assertFalse(SqlUtils.tableExists(connection, "foo"));
    }

    @Test
    public void executeQuery() throws Exception {
        db.prepareTable("xyz", adminClient);
        AtomicReference<String> tableName = new AtomicReference<>();
        db.executeQuery("SELECT * FROM xyz", results -> {
            try {
                tableName.set(results.getMetaData().getTableName(1));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals("xyz", tableName.get().toLowerCase());
    }

    @Test
    public void tableInitialized() throws Exception {
        assertFalse(db.tableInitialized("xyz"));
        db.prepareTable("xyz", adminClient);
        assertTrue(db.tableInitialized("xyz"));
    }
}
