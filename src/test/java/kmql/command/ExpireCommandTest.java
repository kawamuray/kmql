package kmql.command;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.BufferedOutputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import kmql.Database;
import kmql.Engine;

public class ExpireCommandTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();
    @Mock
    private Database db;
    @Mock
    private Engine engine;
    private final ExpireCommand cmd = new ExpireCommand();

    @Before
    public void setUp() {
        doReturn(db).when(engine).db();
    }

    @Test
    public void executeNoArgs() throws SQLException {
        cmd.execute(emptyList(), engine, mock(BufferedOutputStream.class));

        verify(db, times(1)).truncateAllTables();
    }

    @Test
    public void executeWithArgs() throws SQLException {
        List<String> tables = Arrays.asList("foo", "bar");
        cmd.execute(tables, engine, mock(BufferedOutputStream.class));

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(db, times(2)).truncateTable(captor.capture());
        assertEquals(tables, captor.getAllValues());
    }
}
