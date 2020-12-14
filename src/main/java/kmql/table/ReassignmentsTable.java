package kmql.table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.TopicPartition;

import kmql.Table;

public class ReassignmentsTable implements Table {
    @Override
    public String name() {
        return "reassignments";
    }

    @Override
    public void create(Connection connection) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE reassignments ("
                         + "topic VARCHAR(255) NOT NULL,"
                         + "partition INT NOT NULL,"
                         + "replica_id INT NOT NULL,"
                         + "operation ENUM('adding', 'removing') NOT NULL,"
                         + "PRIMARY KEY (topic, partition, replica_id))");
        }
    }

    @Override
    public void prepare(Connection connection, AdminClient adminClient) throws Exception {
        Map<TopicPartition, PartitionReassignment> reassignments =
                adminClient.listPartitionReassignments().reassignments().get();

        try (PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO reassignments (topic, partition, replica_id, operation)"
                + " VALUES (?, ?, ?, ?)")) {
            for (Entry<TopicPartition, PartitionReassignment> entry : reassignments.entrySet()) {
                TopicPartition tp = entry.getKey();
                PartitionReassignment reassignment = entry.getValue();
                for (Integer replica : reassignment.addingReplicas()) {
                    insert(stmt, tp, replica, "adding");
                }
                for (Integer replica : reassignment.removingReplicas()) {
                    insert(stmt, tp, replica, "removing");
                }
            }
        }
    }

    private static void insert(PreparedStatement stmt, TopicPartition tp, int replica, String operation)
            throws SQLException {
        stmt.setString(1, tp.topic());
        stmt.setInt(2, tp.partition());
        stmt.setInt(3, replica);
        stmt.setString(4, operation);
        stmt.executeUpdate();
    }
}
