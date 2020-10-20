package kmql.format;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import kmql.OutputFormat;

public abstract class AbstractCsvFormat implements OutputFormat {
    private final String delimiter;

    protected AbstractCsvFormat(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public void formatTo(ResultSet results, BufferedOutputStream out) throws SQLException, IOException {
        ResultSetMetaData metadata = results.getMetaData();
        int columns = metadata.getColumnCount();

        PrintWriter pw = new PrintWriter(out);
        pw.print("# ");
        String[] fields = new String[columns];
        for (int i = 0; i < columns; i++) {
            fields[i] = metadata.getColumnLabel(i + 1);
        }
        pw.println(String.join(delimiter, fields));

        while (results.next()) {
            for (int i = 0; i < columns; i++) {
                Object value = results.getObject(i + 1);
                fields[i] = String.valueOf(value);
            }
            pw.println(String.join(delimiter, fields));
        }
        pw.flush();
    }
}
