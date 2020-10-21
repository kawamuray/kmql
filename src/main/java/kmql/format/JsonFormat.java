package kmql.format;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import kmql.OutputFormat;

/**
 * JSON format.
 */
public class JsonFormat implements OutputFormat {
    private final ObjectMapper mapper = new ObjectMapper()
            .disable(Feature.AUTO_CLOSE_TARGET);

    @Override
    public void formatTo(ResultSet results, BufferedOutputStream out) throws SQLException, IOException {
        ResultSetMetaData metadata = results.getMetaData();
        int columns = metadata.getColumnCount();

        ArrayNode rowsNode = mapper.createArrayNode();
        while (results.next()) {
            ObjectNode objNode = mapper.createObjectNode();
            for (int i = 1; i <= columns; i++) {
                String label = metadata.getColumnLabel(i);
                Object value = results.getObject(i);
                JsonNode jsonValue = mapper.convertValue(value, JsonNode.class);
                objNode.set(label, jsonValue);
            }
            rowsNode.add(objNode);
        }
        mapper.writeValue(out, rowsNode);
        out.write('\n');
    }
}
