package kmql;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class CommandRegistryTest {
    private final CommandRegistry registry = new CommandRegistry();

    @Test
    public void register() {
        Command command = mock(Command.class);
        registry.register("xyz", command);
        assertSame(command, registry.lookup("xyz").get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void registerTwice() {
        Command command = mock(Command.class);
        registry.register("xyz", command);
        registry.register("xyz", command);
    }
}
