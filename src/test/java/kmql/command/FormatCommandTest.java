package kmql.command;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.BufferedOutputStream;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import kmql.Database;
import kmql.Engine;

public class FormatCommandTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();
    @Mock
    private Database db;
    @Mock
    private Engine engine;
    private final FormatCommand cmd = new FormatCommand();

    @Before
    public void setUp() {
        doReturn(db).when(engine).db();
    }

    @Test
    public void execute() throws SQLException {
        cmd.execute(singletonList("json"), engine, mock(BufferedOutputStream.class));

        verify(engine, times(1)).setOutputFormat("json");
    }

    @Test
    public void executeInvalidArgs() throws SQLException {
        cmd.execute(emptyList(), engine, mock(BufferedOutputStream.class));
        cmd.execute(Arrays.asList("json", "table"), engine, mock(BufferedOutputStream.class));

        verify(engine, never()).setOutputFormat(any(String.class));
    }
}
