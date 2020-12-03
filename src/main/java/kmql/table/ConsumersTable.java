package kmql.table;

import static java.util.stream.Collectors.toList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;

import kmql.Table;

public class ConsumersTable implements Table {
    @Override
    public String name() {
        return "consumers";
    }

    @Override
    public void create(Connection connection) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE consumers ("
                         + "group_id VARCHAR(255) NOT NULL,"
                         + "coordinator_id INT NOT NULL,"
                         + "assignor VARCHAR(255) NOT NULL,"
                         + "state ENUM('Unknown', 'PreparingRebalance', 'CompletingRebalance', 'Stable', 'Dead', 'Empty') NOT NULL,"
                         + "client_id VARCHAR(255) NOT NULL,"
                         + "consumer_id VARCHAR(255) NOT NULL,"
                         + "host VARCHAR(255) NOT NULL,"
                         + "instance_id VARCHAR(255),"
                         + "topic VARCHAR(255) NOT NULL,"
                         + "partition INT NOT NULL,"
                         + "PRIMARY KEY (group_id, consumer_id, topic, partition));");
        }
    }

    @Override
    public void prepare(Connection connection, AdminClient adminClient) throws Exception {
        Collection<ConsumerGroupListing> groups = adminClient.listConsumerGroups().all().get();
        List<String> groupIds = groups.stream().map(ConsumerGroupListing::groupId).collect(toList());
        Map<String, ConsumerGroupDescription> consumerInfos =
                adminClient.describeConsumerGroups(groupIds).all().get();

        try (PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO consumers (group_id, coordinator_id, assignor, state, client_id, consumer_id, host, instance_id, topic, partition)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            for (Entry<String, ConsumerGroupDescription> entry : consumerInfos.entrySet()) {
                String groupId = entry.getKey();
                ConsumerGroupDescription desc = entry.getValue();
                for (MemberDescription member : desc.members()) {
                    for (TopicPartition tp : member.assignment().topicPartitions()) {
                        stmt.setString(1, groupId);
                        stmt.setInt(2, desc.coordinator().id());
                        stmt.setString(3, desc.partitionAssignor());
                        stmt.setString(4, desc.state().toString());
                        stmt.setString(5, member.clientId());
                        stmt.setString(6, member.consumerId());
                        stmt.setString(7, member.host());
                        stmt.setString(8, member.groupInstanceId().orElse(null));
                        stmt.setString(9, tp.topic());
                        stmt.setInt(10, tp.partition());
                        stmt.executeUpdate();
                    }
                }
            }
        }
    }
}
