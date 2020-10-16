package kmql;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlAnalyzer {
    private static final Pattern SQL_TABLE_RE = Pattern.compile("(?:from|join)\\s+\"?([A-Za-z0-9_]+)\"?");

    private SqlAnalyzer() {}

    public static List<String> requiredTables(String sql) {
        Matcher matcher = SQL_TABLE_RE.matcher(sql.toLowerCase());
        List<String> tables = new ArrayList<>();
        while (matcher.find()) {
            String table = matcher.group(1);
            tables.add(table);
        }
        return tables;
    }
}
