package kmql;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import kmql.table.BrokersTable;
import kmql.table.LogdirsTable;
import kmql.table.ReplicasTable;

public class TableRegistry implements Iterable<Map.Entry<String, Table>> {
    public static final TableRegistry DEFAULT = new TableRegistry();

    static {
        registerDefault(new ReplicasTable());
        registerDefault(new BrokersTable());
        registerDefault(new LogdirsTable());
    }

    private final ConcurrentMap<String, Table> tables;

    public static void registerDefault(Table table) {
        DEFAULT.register(table.name(), table);
    }

    public static void registerDefault(String name, Table table) {
        DEFAULT.register(name, table);
    }

    public TableRegistry() {
        tables = new ConcurrentHashMap<>();
    }

    public void register(String name, Table table) {
        if (tables.putIfAbsent(name, table) != null) {
            throw new IllegalArgumentException("conflicting table name: " + name);
        }
    }

    public Optional<Table> lookup(String name) {
        return Optional.ofNullable(tables.get(name));
    }

    @Override
    public Iterator<Entry<String, Table>> iterator() {
        return tables.entrySet().iterator();
    }
}
