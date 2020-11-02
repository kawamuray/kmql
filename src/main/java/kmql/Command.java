package kmql;

import java.io.BufferedOutputStream;
import java.util.List;

/**
 * An interface of the kmql meta commands.
 */
public interface Command {
    /**
     * Return the help string of this command.
     * @return help string of this command.
     */
    String help();

    /**
     * Execute this command.
     * @param args command arguments.
     * @param engine a {@link Engine} of the executing context.
     * @param output output stream to write any outputs.
     */
    void execute(List<String> args, Engine engine, BufferedOutputStream output);
}
