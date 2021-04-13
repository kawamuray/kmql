package kmql.table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import kmql.Table;

public class ReplicasTable implements Table {
    @Override
    public String name() {
        return "replicas";
    }

    @Override
    public void create(Connection connection) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE replicas ("
                         + "topic VARCHAR(255) NOT NULL,"
                         + "partition INT NOT NULL,"
                         + "broker_id INT NOT NULL,"
                         + "is_leader BOOLEAN NOT NULL,"
                         + "is_preferred_leader BOOLEAN NOT NULL,"
                         + "is_in_sync BOOLEAN NOT NULL,"
                         + "replica_order INT NOT NULL,"
                         + "PRIMARY KEY (topic, partition, broker_id))");
        }
    }

    @Override
    public void prepare(Connection connection, AdminClient adminClient) throws Exception {
        ListTopicsOptions options = new ListTopicsOptions().listInternal(true);
        Set<String> topics = adminClient.listTopics(options).names().get();
        Map<String, TopicDescription> topicInfo = adminClient.describeTopics(topics).all().get();
        try (PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO replicas (topic, partition, broker_id, is_leader, is_preferred_leader, is_in_sync, replica_order)"
                + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            for (String topic : topics) {
                TopicDescription desc = topicInfo.get(topic);
                for (TopicPartitionInfo partition : desc.partitions()) {
                    List<Node> replicas = partition.replicas();
                    for (int i = 0; i < replicas.size(); i++) {
                        Node replica = replicas.get(i);
                        stmt.setString(1, topic);
                        stmt.setInt(2, partition.partition());
                        stmt.setInt(3, replica.id());
                        Node leader = partition.leader();
                        stmt.setBoolean(4, leader != null && leader.id() == replica.id());
                        stmt.setBoolean(5, replicas.get(0).id() == replica.id());
                        stmt.setBoolean(6, partition.isr().contains(replica));
                        stmt.setInt(7, i);
                        stmt.executeUpdate();
                    }
                }
            }
        }
    }
}
