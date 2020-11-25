package kmql.table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Collection;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;

import kmql.Table;

public class BrokersTable implements Table {
    @Override
    public String name() {
        return "brokers";
    }

    @Override
    public void create(Connection connection) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE brokers ("
                         + "id INT NOT NULL,"
                         + "host VARCHAR(255) NOT NULL,"
                         + "port INT NOT NULL,"
                         + "rack VARCHAR(255),"
                         + "is_controller BOOLEAN NOT NULL,"
                         + "PRIMARY KEY (id))");
        }
    }

    @Override
    public void prepare(Connection connection, AdminClient adminClient) throws Exception {
        DescribeClusterResult describeResult = adminClient.describeCluster();
        Collection<Node> nodes = describeResult.nodes().get();
        Node controllerNode = describeResult.controller().get();
        try (PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO brokers (id, host, port, rack, is_controller) VALUES (?, ?, ?, ?, ?)")) {
            for (Node node : nodes) {
                stmt.setInt(1, node.id());
                stmt.setString(2, node.host());
                stmt.setInt(3, node.port());
                stmt.setString(4, node.rack());
                stmt.setBoolean(5, node.id() == controllerNode.id());
                stmt.executeUpdate();
            }
        }
    }
}
