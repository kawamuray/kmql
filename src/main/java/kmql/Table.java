package kmql;

import static java.util.Collections.emptyList;

import java.sql.Connection;
import java.util.Collection;

import org.apache.kafka.clients.admin.AdminClient;

/**
 * An interface of a kmql table implementation.
 */
public interface Table {
    /**
     * Name of the table.
     * @return name of the table.
     */
    String name();

    /**
     * Optionally declared list of table names that this table is depending on to construct the table.
     * The returned tables are prepared before the {@link #prepare(Connection, AdminClient)} method of this
     * table is called.
     * @return list of table names to depend on.
     */
    default Collection<String> dependencyTables() {
        return emptyList();
    }

    /**
     * Create this table on the given {@link Connection}.
     * It's guaranteed that this method is called only once per kmql session.
     * @param connection a JDBC {@link Connection}.
     * @throws Exception at any errors.
     */
    void create(Connection connection) throws Exception;

    /**
     * Prepare this table on the given {@link Connection} by feeding in its data by
     * obtaining it from the given {@link AdminClient}.
     * At the time of this method call, it's guaranteed that the target table exists and it's' empty.
     * @param connection a JDBC {@link Connection}.
     * @param adminClient a Kafka {@link AdminClient}.
     * @throws Exception at any errors.
     */
    void prepare(Connection connection, AdminClient adminClient) throws Exception;
}
