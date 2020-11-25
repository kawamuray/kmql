package kmql.table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;

import kmql.Table;

public class ConfigsTable implements Table {
    @Override
    public String name() {
        return "configs";
    }

    @Override
    public Collection<String> dependencyTables() {
        return Arrays.asList("brokers", "replicas");
    }

    @Override
    public void create(Connection connection) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE configs ("
                         + "resource_type ENUM('broker_logger', 'broker', 'topic', 'unknown') NOT NULL,"
                         + "name VARCHAR(255) NOT NULL,"
                         + "key VARCHAR(255) NOT NULL,"
                         + "value VARCHAR(255),"
                         + "source ENUM('dynamic_topic_config', 'dynamic_broker_logger_config', 'dynamic_broker_config',"
                         + "            'dynamic_default_broker_config', 'static_broker_config', 'default_config', 'unknown'),"
                         + "is_default BOOLEAN NOT NULL,"
                         + "is_sensitive BOOLEAN NOT NULL,"
                         + "PRIMARY KEY (resource_type, name, key))");
        }
    }

    @Override
    public void prepare(Connection connection, AdminClient adminClient) throws Exception {
        List<ConfigResource> resources = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet results = stmt.executeQuery("SELECT id FROM brokers")) {
            while (results.next()) {
                int brokerId = results.getInt(1);
                resources.add(new ConfigResource(Type.BROKER, String.valueOf(brokerId)));
            }
        }
        try (Statement stmt = connection.createStatement();
             ResultSet results = stmt.executeQuery("SELECT DISTINCT topic FROM replicas")) {
            while (results.next()) {
                String topic = results.getString(1);
                resources.add(new ConfigResource(Type.TOPIC, topic));
            }
        }

        Map<ConfigResource, Config> configs = adminClient.describeConfigs(resources).all().get();
        try (PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO configs (resource_type, name, key, value, source, is_default, is_sensitive)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            for (Entry<ConfigResource, Config> entry : configs.entrySet()) {
                ConfigResource resource = entry.getKey();
                Config entries = entry.getValue();
                for (ConfigEntry config : entries.entries()) {
                    stmt.setString(1, resource.type().name().toLowerCase());
                    stmt.setString(2, resource.name());
                    stmt.setString(3, config.name());
                    stmt.setString(4, config.value());
                    stmt.setString(5, config.source().name().toLowerCase());
                    stmt.setBoolean(6, config.isDefault());
                    stmt.setBoolean(7, config.isSensitive());
                    stmt.executeUpdate();
                }
            }
        }
    }
}
