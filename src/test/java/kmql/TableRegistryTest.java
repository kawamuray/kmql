package kmql;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import org.junit.Test;

public class TableRegistryTest {
    private final TableRegistry registry = new TableRegistry();

    @Test
    public void register() {
        Table table = mock(Table.class);
        registry.register("xyz", table);
        assertSame(table, registry.lookup("xyz").get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void registerTwice() {
        Table table = mock(Table.class);
        registry.register("xyz", table);
        registry.register("xyz", table);
    }
}
