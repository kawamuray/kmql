package kmql.command;

import java.io.BufferedOutputStream;
import java.io.PrintWriter;
import java.util.List;

import kmql.Command;
import kmql.Engine;

/**
 * Expire table caches.
 */
public class ExpireCommand implements Command {
    @Override
    public String help() {
        return ":expire - Expire all initialized tables (tables are re-created when next time they're queried)\n"
               +
               ":expire TABLE1[ TABLE2...] - Expire specified tables";
    }

    @Override
    public void execute(List<String> args, Engine engine, BufferedOutputStream output) {
        PrintWriter pw = new PrintWriter(output);
        try {
            if (args.isEmpty()) {
                pw.println("Expiring ALL tables...");
                engine.db().truncateAllTables();
            } else {
                for (String table : args) {
                    pw.printf("Expiring table %s...\n", table);
                    engine.db().truncateTable(table);
                }
            }
        } catch (Exception e) {
            pw.println("Failed to truncate table: " + e.getMessage());
        }
        pw.flush();
    }
}
