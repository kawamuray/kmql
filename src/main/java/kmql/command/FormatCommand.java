package kmql.command;

import java.io.BufferedOutputStream;
import java.io.PrintWriter;
import java.util.List;

import kmql.Command;
import kmql.Engine;

/**
 * Expire table caches.
 */
public class FormatCommand implements Command {
    @Override
    public String help() {
        return ":format FORMAT - Set output format";
    }

    @Override
    public void execute(List<String> args, Engine engine, BufferedOutputStream output) {
        PrintWriter pw = new PrintWriter(output);
        try {
            if (args.size() != 1) {
                pw.println("Error usage: :format FORMAT");
                return;
            }
            String format = args.get(0);
            try {
                engine.setOutputFormat(format);
                pw.printf("Output format set to '%s'\n", format);
            } catch (IllegalArgumentException ignored) {
                pw.printf("Error: no such format '%s'\n", format);
            }
        } finally {
            pw.flush();
        }
    }
}
