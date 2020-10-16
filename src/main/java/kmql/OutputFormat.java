package kmql;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface OutputFormat {
    void formatTo(ResultSet results, BufferedOutputStream out) throws SQLException, IOException;
}
