package kmql;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class OutputFormatRegistryTest {
    private final OutputFormatRegistry registry = new OutputFormatRegistry();

    @Test
    public void register() {
        OutputFormat format = mock(OutputFormat.class);
        registry.register("foo", format);
        assertSame(format, registry.lookup("foo").get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void registerConflict() {
        OutputFormat format = mock(OutputFormat.class);
        registry.register("foo", format);
        registry.register("foo", format);
    }
}
