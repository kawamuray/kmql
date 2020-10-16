package kmql;

import java.sql.Connection;

import org.apache.kafka.clients.admin.AdminClient;

public interface Table {
    String name();

    void prepare(Connection connection, AdminClient adminClient) throws Exception;
}
