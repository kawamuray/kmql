package kmql;

import java.sql.ResultSet;

import org.h2.tools.SimpleResultSet;

import lombok.Value;

public class SqlUtils {
    private SqlUtils() {}

    @Value
    public static class ColumnInfo {
        String name;
        int sqlType;
    }

    public static ResultSet resultSet(ColumnInfo[] columns, Object[]... rows) {
        SimpleResultSet results = new SimpleResultSet();
        for (ColumnInfo column : columns) {
            results.addColumn(column.name, column.sqlType, Integer.MAX_VALUE, 0);
        }
        for (Object[] row : rows) {
            results.addRow(row);
        }
        return results;
    }
}
