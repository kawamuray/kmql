package kmql;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

public class SqlAnalyzerTest {
    @Test
    public void requiredTables() {
        assertEquals(singletonList("xyz"), SqlAnalyzer.requiredTables("SELECT * FROM xyz WHERE x = 10"));
        assertEquals(Arrays.asList("xyz", "foo"),
                     SqlAnalyzer.requiredTables("SELECT * FROM xyz LEFT JOIN foo"));
        assertEquals(singletonList("xyz"), SqlAnalyzer.requiredTables("select * from xyz where x = 10"));
        assertEquals(Arrays.asList("xyz", "foo"),
                     SqlAnalyzer.requiredTables("select * from xyz left join foo"));
    }
}
