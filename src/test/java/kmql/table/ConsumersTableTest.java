package kmql.table;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import kmql.SqlUtils;
import lombok.Value;

public class ConsumersTableTest {
    @Rule
    public final MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private AdminClient adminClient;

    private final ConsumersTable table = new ConsumersTable();

    @Value
    private static class Row {
        String groupId;
        int coordinator;
        String assignor;
        String state;
        String clientId;
        String consumerId;
        String host;
        String instanceId;
        String topic;
        int partition;

        static Row fromResults(ResultSet results) throws SQLException {
            return new Row(
                    results.getString(1),
                    results.getInt(2),
                    results.getString(3),
                    results.getString(4),
                    results.getString(5),
                    results.getString(6),
                    results.getString(7),
                    results.getString(8),
                    results.getString(9),
                    results.getInt(10));
        }
    }

    private void mockAdminClient(Map<String, ConsumerGroupDescription> descMap) {
        ListConsumerGroupsResult listResult = mock(ListConsumerGroupsResult.class);
        List<ConsumerGroupListing> groups = descMap.keySet().stream()
                                                   .map(k -> new ConsumerGroupListing(k, false))
                                                   .collect(Collectors.toList());
        KafkaFuture<List<ConsumerGroupListing>> listFuture = KafkaFuture.completedFuture(groups);
        doReturn(listFuture).when(listResult).all();
        doReturn(listResult).when(adminClient).listConsumerGroups();

        DescribeConsumerGroupsResult describeResult = mock(DescribeConsumerGroupsResult.class);
        KafkaFuture<Map<String, ConsumerGroupDescription>> describeFuture = KafkaFuture.completedFuture(
                descMap);
        doReturn(describeFuture).when(describeResult).all();
        doReturn(describeResult).when(adminClient).describeConsumerGroups(any());
    }

    @Test
    public void prepare() throws Exception {
        Map<String, ConsumerGroupDescription> descMap = new HashMap<>();
        descMap.put("group1", new ConsumerGroupDescription(
                "group1",
                false,
                Arrays.asList(
                        new MemberDescription("member1", "client1", "host1.com",
                                              new MemberAssignment(
                                                      new HashSet<>(Arrays.asList(
                                                              new TopicPartition("topicA", 1),
                                                              new TopicPartition("topicB", 2)
                                                      ))
                                              )),
                        new MemberDescription("member2", "client2", "host2.com",
                                              new MemberAssignment(
                                                      new HashSet<>(Arrays.asList(
                                                              new TopicPartition("topicA", 2)))))
                ),
                "range",
                ConsumerGroupState.STABLE,
                new Node(1, "broker1.com", 1234)));
        descMap.put("group2", new ConsumerGroupDescription(
                "group2",
                false,
                Arrays.asList(
                        new MemberDescription("member3", "client3", "host3.com",
                                              new MemberAssignment(
                                                      new HashSet<>(Arrays.asList(
                                                              new TopicPartition("topicA", 1)
                                                      ))
                                              ))
                ),
                "range",
                ConsumerGroupState.PREPARING_REBALANCE,
                new Node(2, "broker2.com", 1234)));

        mockAdminClient(descMap);

        List<Row> rows = new ArrayList<>();
        try (Connection connection = SqlUtils.connection()) {
            table.create(connection);
            table.prepare(connection, adminClient);

            try (Statement stmt = connection.createStatement();
                 ResultSet results = stmt.executeQuery(
                         "SELECT * FROM consumers ORDER BY (group_id, consumer_id)")) {
                while (results.next()) {
                    Row row = Row.fromResults(results);
                    rows.add(row);
                }
            }
        }

        List<Row> expected = Arrays.asList(
                new Row("group1", 1, "range", ConsumerGroupState.STABLE.toString(),
                        "client1", "member1", "host1.com", null, "topicA", 1),
                new Row("group1", 1, "range", ConsumerGroupState.STABLE.toString(),
                        "client1", "member1", "host1.com", null, "topicB", 2),
                new Row("group1", 1, "range", ConsumerGroupState.STABLE.toString(),
                        "client2", "member2", "host2.com", null, "topicA", 2),
                new Row("group2", 2, "range", ConsumerGroupState.PREPARING_REBALANCE.toString(),
                        "client3", "member3", "host3.com", null, "topicA", 1)
        );
        assertEquals(expected, rows);
    }

    @Test
    public void prepareAllEnums() throws Exception {
        Map<String, ConsumerGroupDescription> descMap = new HashMap<>();
        for (ConsumerGroupState state : ConsumerGroupState.values()) {
            String group = "group-" + state;
            descMap.put(group, new ConsumerGroupDescription(
                    group,
                    false,
                    Arrays.asList(
                            new MemberDescription("member1", "client1", "host1.com",
                                                  new MemberAssignment(
                                                          new HashSet<>(Arrays.asList(
                                                                  new TopicPartition("topicA", 1)
                                                          ))
                                                  ))),
                    "range",
                    state,
                    new Node(1, "broker1.com", 1234)));

        }

        mockAdminClient(descMap);

        try (Connection connection = SqlUtils.connection()) {
            table.create(connection);
            table.prepare(connection, adminClient);

            try (Statement stmt = connection.createStatement();
                 ResultSet results = stmt.executeQuery(
                         "SELECT COUNT(*) FROM consumers")) {
                results.next();
                int count = results.getInt(1);
                assertEquals(ConsumerGroupState.values().length, count);
            }
        }
    }
}
