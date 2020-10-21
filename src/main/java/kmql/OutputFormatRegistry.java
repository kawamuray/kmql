package kmql;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import kmql.format.JsonFormat;
import kmql.format.SsvFormat;
import kmql.format.TableFormat;

/**
 * Registry of outputs formats.
 */
public class OutputFormatRegistry {
    public static final OutputFormatRegistry DEFAULT = new OutputFormatRegistry();

    static {
        registerDefault("table", new TableFormat());
        registerDefault("json", new JsonFormat());
        registerDefault("ssv", new SsvFormat());
    }

    private final ConcurrentMap<String, OutputFormat> formats;

    public OutputFormatRegistry() {
        formats = new ConcurrentHashMap<>();
    }

    /**
     * Register the given format under the given name to the default registry.
     * @param name the name of the format.
     * @param format the format instance.
     */
    public static void registerDefault(String name, OutputFormat format) {
        DEFAULT.register(name, format);
    }

    /**
     * Register the given format under the given name.
     * @param name the name of the format.
     * @param format the format instance.
     */
    public void register(String name, OutputFormat format) {
        if (formats.putIfAbsent(name, format) != null) {
            throw new IllegalArgumentException("conflicting format name: " + name);
        }
    }

    /**
     * Lookup a format by the name.
     * @param name the name of the format.
     * @return an {@link OutputFormat} if presents.
     */
    public Optional<OutputFormat> lookup(String name) {
        return Optional.ofNullable(formats.get(name));
    }
}
