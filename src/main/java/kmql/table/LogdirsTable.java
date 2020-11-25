package kmql.table;

import static java.util.Collections.singletonList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.ReplicaInfo;

import kmql.Table;

public class LogdirsTable implements Table {
    @Override
    public String name() {
        return "logdirs";
    }

    @Override
    public Collection<String> dependencyTables() {
        return singletonList("brokers");
    }

    @Override
    public void create(Connection connection) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE logdirs ("
                         + "broker_id INT NOT NULL,"
                         + "path VARCHAR(255) NOT NULL,"
                         + "topic VARCHAR(255) NOT NULL,"
                         + "partition INT NOT NULL,"
                         + "size BIGINT NOT NULL,"
                         + "offset_lag BIGINT NOT NULL,"
                         + "is_future BOOLEAN NOT NULL,"
                         + "PRIMARY KEY (broker_id, path, topic, partition))");
        }
    }

    @Override
    public void prepare(Connection connection, AdminClient adminClient) throws Exception {
        List<Integer> brokerIds = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
            ResultSet results = stmt.executeQuery("SELECT id FROM brokers")) {
            while (results.next()) {
                int brokerId = results.getInt(1);
                brokerIds.add(brokerId);
            }
        }

        Map<Integer, Map<String, LogDirInfo>> logDirs = adminClient.describeLogDirs(brokerIds).all().get();
        try (PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO logdirs (broker_id, path, topic, partition, size, offset_lag, is_future) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            for (Entry<Integer, Map<String, LogDirInfo>> entry : logDirs.entrySet()) {
                int brokerId = entry.getKey();
                for (Entry<String, LogDirInfo> dirEntry : entry.getValue().entrySet()) {
                    String path = dirEntry.getKey();
                    LogDirInfo info = dirEntry.getValue();
                    for (Entry<TopicPartition, ReplicaInfo> replicaEntry : info.replicaInfos.entrySet()) {
                        TopicPartition tp = replicaEntry.getKey();
                        ReplicaInfo replicaInfo = replicaEntry.getValue();
                        stmt.setInt(1, brokerId);
                        stmt.setString(2, path);
                        stmt.setString(3, tp.topic());
                        stmt.setInt(4, tp.partition());
                        stmt.setLong(5, replicaInfo.size);
                        stmt.setLong(6, replicaInfo.offsetLag);
                        stmt.setBoolean(7, replicaInfo.isFuture);
                        stmt.executeUpdate();
                    }
                }
            }
        }
    }
}
