package kmql;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import kmql.table.BrokersTable;
import kmql.table.ConfigsTable;
import kmql.table.ConsumersTable;
import kmql.table.LogdirsTable;
import kmql.table.ReassignmentsTable;
import kmql.table.ReplicasTable;

/**
 * Registry of kmql tables.
 */
public class TableRegistry implements Iterable<Map.Entry<String, Table>> {
    public static final TableRegistry DEFAULT = new TableRegistry();

    static {
        registerDefault(new ReplicasTable());
        registerDefault(new BrokersTable());
        registerDefault(new LogdirsTable());
        registerDefault(new ConfigsTable());
        registerDefault(new ConsumersTable());
        registerDefault(new ReassignmentsTable());
    }

    private final ConcurrentMap<String, Table> tables;

    /**
     * Register the given table under the {@link Table#name()} to the default registry.
     * @param table the table instance.
     */
    public static void registerDefault(Table table) {
        DEFAULT.register(table.name(), table);
    }

    /**
     * Register the given format under the given name to the default registry.
     * @param name the name of the table.
     * @param table the table instance.
     */
    public static void registerDefault(String name, Table table) {
        DEFAULT.register(name, table);
    }

    public TableRegistry() {
        tables = new ConcurrentHashMap<>();
    }

    /**
     * Register the given table under the given name.
     * @param name the name of the table.
     * @param table the table instance.
     */
    public void register(String name, Table table) {
        if (tables.putIfAbsent(name, table) != null) {
            throw new IllegalArgumentException("conflicting table name: " + name);
        }
    }

    /**
     * Lookup a table by the name.
     * @param name the name of the table.
     * @return an {@link Table} if presents.
     */
    public Optional<Table> lookup(String name) {
        return Optional.ofNullable(tables.get(name));
    }

    /**
     * Returns the iterator over table entries registered in this registry.
     * @return an iterator for table name and table instances.
     */
    @Override
    public Iterator<Entry<String, Table>> iterator() {
        return tables.entrySet().iterator();
    }
}
