package kmql;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import kmql.format.JsonFormat;
import kmql.format.TableFormat;

public class OutputFormatRegistry {
    public static final OutputFormatRegistry DEFAULT = new OutputFormatRegistry();

    static {
        registerDefault("table", new TableFormat());
        registerDefault("json", new JsonFormat());
    }

    private final ConcurrentMap<String, OutputFormat> formats;

    public OutputFormatRegistry() {
        formats = new ConcurrentHashMap<>();
    }

    public static void registerDefault(String name, OutputFormat format) {
        DEFAULT.register(name, format);
    }

    public void register(String name, OutputFormat format) {
        if (formats.putIfAbsent(name, format) != null) {
            throw new IllegalArgumentException("conflicting format name: " + name);
        }
    }

    public Optional<OutputFormat> lookup(String name) {
        return Optional.ofNullable(formats.get(name));
    }
}
