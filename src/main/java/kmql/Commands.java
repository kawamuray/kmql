package kmql;

import java.io.BufferedOutputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Entrypoint to execute meta commands.
 */
public class Commands {
    public static final String COMMAND_PREFIX = ":";

    private Commands() {}

    /**
     * Return true if the given line is intending to execute meta command.
     * @param line a line to test.
     * @return true if the given line is a meta command line.
     */
    public static boolean isCommand(String line) {
        return line.trim().startsWith(COMMAND_PREFIX);
    }

    /**
     * Execute the given line by parsing, looking up and executing command.
     * @param engine an {@link Engine} of executing context.
     * @param output a stream to write command output.
     * @param line meta command line.
     */
    public static void executeLine(Engine engine, BufferedOutputStream output, String line) {
        List<String> cmdline = parseLine(line);
        Command command = engine.commandRegistry().lookup(cmdline.get(0)).orElseThrow(
                () -> new IllegalArgumentException("no such command: " + cmdline.get(0)));
        command.execute(cmdline.subList(1, cmdline.size()), engine, output);
    }

    private static List<String> parseLine(String line) {
        List<String> cmdline = Arrays.asList(line.split("\\s+"));
        String cmd = cmdline.get(0);
        String commandName = cmd.substring(COMMAND_PREFIX.length()).trim();
        cmdline.set(0, commandName);
        return cmdline;
    }
}
