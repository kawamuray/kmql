package kmql;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * An interface of the kmql output format implementation.
 */
public interface OutputFormat {
    /**
     * Format the given {@link ResultSet} and write it into {@link BufferedOutputStream}.
     * @param results the SQL query result.
     * @param out the output stream to write the output.
     * @throws SQLException if {@link ResultSet} throws.
     * @throws IOException if {@link BufferedOutputStream} throws.
     */
    void formatTo(ResultSet results, BufferedOutputStream out) throws SQLException, IOException;
}
