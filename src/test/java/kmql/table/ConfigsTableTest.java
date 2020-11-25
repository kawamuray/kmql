package kmql.table;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import kmql.SqlUtils;

public class ConfigsTableTest {
    @Rule
    public final MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private AdminClient adminClient;

    private final ConfigsTable table = new ConfigsTable();

    @Test
    public void prepareAllEnums() throws Exception {
        Map<ConfigResource, Config> configs = new HashMap<>();
        for (Type type : Type.values()) {
            List<ConfigEntry> entries = new ArrayList<>();
            for (ConfigSource source : ConfigSource.values()) {
                ConfigEntry entry = mock(ConfigEntry.class);
                doReturn("retention.ms-" + source).when(entry).name();
                doReturn("1234").when(entry).value();
                doReturn(source).when(entry).source();
                entries.add(entry);
            }
            configs.put(new ConfigResource(type, "xyz"), new Config(entries));
        }
        KafkaFuture<Object> future = KafkaFuture.completedFuture(configs);
        DescribeConfigsResult result = mock(DescribeConfigsResult.class);
        doReturn(future).when(result).all();
        doReturn(result).when(adminClient).describeConfigs(any());

        try (Connection connection = SqlUtils.connection()) {
            // Prepare empty dependency tables
            new BrokersTable().create(connection);
            new ReplicasTable().create(connection);

            table.create(connection);
            table.prepare(connection, adminClient);

            try (Statement stmt = connection.createStatement();
                 ResultSet results = stmt.executeQuery("SELECT COUNT(*) FROM configs")) {
                results.next();
                int count = results.getInt(1);
                assertEquals(Type.values().length * ConfigSource.values().length, count);
            }
        }
    }
}
