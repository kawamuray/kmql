package kmql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.SimpleResultSet;

import lombok.Value;

public class SqlUtils {
    private SqlUtils() {}

    @Value
    public static class ColumnInfo {
        String name;
        int sqlType;
    }

    public static Connection connection() {
        try {
            Class.forName("org.h2.Driver");
            return DriverManager.getConnection("jdbc:h2:mem:");
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean tableExists(Connection connection, String name) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet results = stmt.executeQuery("SHOW TABLES")) {
            while (results.next()) {
                String table = results.getString(1);
                if (table.toLowerCase().equals(name.toLowerCase())) {
                    return true;
                }
            }
        }
        return false;
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
