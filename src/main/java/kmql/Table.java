package kmql;

import static java.util.Collections.emptyList;

import java.sql.Connection;
import java.util.Collection;

import org.apache.kafka.clients.admin.AdminClient;

public interface Table {
    String name();

    default Collection<String> dependencyTables() {
        return emptyList();
    }

    void prepare(Connection connection, AdminClient adminClient) throws Exception;
}
