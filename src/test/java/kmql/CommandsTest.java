package kmql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.junit.Test;

public class CommandsTest {
    private final Command xyzCommand = new Command() {
        @Override
        public String help() {
            return ":xyz";
        }

        @Override
        public void execute(List<String> args, Engine engine, BufferedOutputStream output) {
            PrintWriter pw = new PrintWriter(output);
            pw.printf("xyz,%s", String.join(",", args));
            pw.flush();
        }
    };

    private final CommandRegistry registry = new CommandRegistry() {{
        register("xyz", xyzCommand);
    }};

    @Test
    public void isCommand() {
        assertTrue(Commands.isCommand(":help"));
        assertTrue(Commands.isCommand(":help arg1 arg2"));
        assertTrue(Commands.isCommand("   :help   "));
        assertFalse(Commands.isCommand("SELECT * FROM :xyz"));
        assertFalse(Commands.isCommand("   SELECT * FROM table ;"));
    }

    @Test
    public void executeLine() throws IOException {
        Engine engine = mock(Engine.class);
        doReturn(registry).when(engine).commandRegistry();

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (BufferedOutputStream bout = new BufferedOutputStream(output)) {
            Commands.executeLine(engine, bout, ":xyz foo bar");
        }
        assertEquals("xyz,foo,bar", new String(output.toByteArray()));
    }
}
