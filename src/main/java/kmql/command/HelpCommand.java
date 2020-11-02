package kmql.command;

import java.io.BufferedOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import kmql.Command;
import kmql.Engine;

/**
 * Show help for interactive console.
 */
public class HelpCommand implements Command {
    @Override
    public String help() {
        return ":help - Show this help";
    }

    @Override
    public void execute(List<String> args, Engine engine, BufferedOutputStream output) {
        PrintWriter pw = new PrintWriter(output);
        pw.println("Execute SQL:");
        pw.println("  SELECT * FROM $table WHERE condA = x LIMIT 3;");
        pw.println("Meta commands:");
        List<String> helpLines = new ArrayList<>();
        for (Entry<String, Command> entry : engine.commandRegistry()) {
            helpLines.add(entry.getValue().help());
        }
        Collections.sort(helpLines);
        for (String helpLine : helpLines) {
            pw.println(helpLine.trim());
        }
        pw.flush();
    }
}
