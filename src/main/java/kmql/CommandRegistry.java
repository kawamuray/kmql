package kmql;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import kmql.command.ExpireCommand;
import kmql.command.FormatCommand;
import kmql.command.HelpCommand;

/**
 * Registry of meta commands.
 */
public class CommandRegistry implements Iterable<Entry<String, Command>> {
    public static final CommandRegistry DEFAULT = new CommandRegistry();

    static {
        registerDefault("help", new HelpCommand());
        registerDefault("expire", new ExpireCommand());
        registerDefault("format", new FormatCommand());
    }

    private final ConcurrentMap<String, Command> commands;

    public CommandRegistry() {
        commands = new ConcurrentHashMap<>();
    }

    /**
     * Register the given command under the given name to the default registry.
     * @param name the name of the command.
     * @param command the command instance.
     */
    public static void registerDefault(String name, Command command) {
        DEFAULT.register(name, command);
    }

    /**
     * Register the given command under the given name.
     * @param name the name of the command.
     * @param command the command instance.
     */
    public void register(String name, Command command) {
        if (commands.putIfAbsent(name, command) != null) {
            throw new IllegalArgumentException("conflicting command name: " + name);
        }
    }

    /**
     * Lookup a command by the name.
     * @param name the name of the command.
     * @return an {@link Command} if presents.
     */
    public Optional<Command> lookup(String name) {
        return Optional.ofNullable(commands.get(name));
    }

    /**
     * Returns the iterator over command entries registered in this registry.
     * @return an iterator for command name and command instances.
     */
    @Override
    public Iterator<Entry<String, Command>> iterator() {
        return commands.entrySet().iterator();
    }
}
