package kmql.format;

import java.io.BufferedOutputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.jakewharton.fliptables.FlipTable;

import kmql.OutputFormat;

/**
 * Pretty-printed table format.
 */
public class TableFormat implements OutputFormat {
    public static final String[][] ARRAY_PROTO = new String[0][];

    @Override
    public void formatTo(ResultSet results, BufferedOutputStream out) throws SQLException {
        ResultSetMetaData metadata = results.getMetaData();
        int columns = metadata.getColumnCount();
        String[] headers = new String[columns];
        for (int i = 0; i < columns; i++) {
            headers[i] = metadata.getColumnLabel(i + 1);
        }

        List<String[]> rows = new ArrayList<>();
        while (results.next()) {
            String[] row = new String[columns];
            for (int i = 0; i < columns; i++) {
                Object value = results.getObject(i + 1);
                row[i] = String.valueOf(value);
            }
            rows.add(row);
        }

        PrintWriter pw = new PrintWriter(out);
        pw.write(FlipTable.of(headers, rows.toArray(ARRAY_PROTO)));
        pw.flush();
    }
}
