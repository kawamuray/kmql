package kmql.table;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import kmql.SqlUtils;
import lombok.Value;

public class ReassignmentsTableTest {
    @Rule
    public final MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private AdminClient adminClient;

    private final ReassignmentsTable table = new ReassignmentsTable();

    @Value
    private static class Row {
        String topic;
        int partition;
        int replica;
        String operation;

        static Row fromResults(ResultSet results) throws SQLException {
            return new Row(
                    results.getString(1),
                    results.getInt(2),
                    results.getInt(3),
                    results.getString(4));
        }
    }

    @Test
    public void prepare() throws Exception {
        Map<TopicPartition, PartitionReassignment> reassignments = new HashMap<>();
        reassignments.put(new TopicPartition("topic", 1), new PartitionReassignment(
                Arrays.asList(1, 2),
                Arrays.asList(1),
                Arrays.asList(2)));
        reassignments.put(new TopicPartition("topic", 2), new PartitionReassignment(
                Arrays.asList(3),
                emptyList(),
                Arrays.asList(3)));
        ListPartitionReassignmentsResult result = mock(ListPartitionReassignmentsResult.class);
        doReturn(KafkaFuture.completedFuture(reassignments)).when(result).reassignments();
        doReturn(result).when(adminClient).listPartitionReassignments();

        List<Row> rows = new ArrayList<>();
        try (Connection connection = SqlUtils.connection()) {
            table.create(connection);
            table.prepare(connection, adminClient);

            try (Statement stmt = connection.createStatement();
                 ResultSet results = stmt.executeQuery(
                         "SELECT * FROM reassignments ORDER BY (topic, partition, replica_id)")) {
                while (results.next()) {
                    Row row = Row.fromResults(results);
                    rows.add(row);
                }
            }
        }

        List<Row> expected = Arrays.asList(
                new Row("topic", 1, 1, "adding"),
                new Row("topic", 1, 2, "removing"),
                new Row("topic", 2, 3, "removing"));
        assertEquals(expected, rows);
    }
}
