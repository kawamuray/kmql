package kmql.format;

import static org.junit.Assert.assertEquals;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.sql.ResultSet;
import java.sql.Types;

import org.junit.Test;

import kmql.SqlUtils;
import kmql.SqlUtils.ColumnInfo;

public class JsonFormatTest {

    @Test
    public void formatTo() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BufferedOutputStream bout = new BufferedOutputStream(out);
        ResultSet results = SqlUtils.resultSet(new ColumnInfo[] {
                                                       new ColumnInfo("ID", Types.INTEGER),
                                                       new ColumnInfo("HOST", Types.VARCHAR),
                                                       new ColumnInfo("IS_CONTROLLER", Types.BOOLEAN),
                                                       },
                                               new Object[] { 1, "host1.com", true },
                                               new Object[] { 2, "host2.com", false },
                                               new Object[] { 3, "host3.com", false });
        new JsonFormat().formatTo(results, bout);
        bout.flush();
        String expected = "[{\"ID\":1,\"HOST\":\"host1.com\",\"IS_CONTROLLER\":true}," +
                          "{\"ID\":2,\"HOST\":\"host2.com\",\"IS_CONTROLLER\":false}," +
                          "{\"ID\":3,\"HOST\":\"host3.com\",\"IS_CONTROLLER\":false}]\n";
        assertEquals(expected, new String(out.toByteArray()));
    }
}
