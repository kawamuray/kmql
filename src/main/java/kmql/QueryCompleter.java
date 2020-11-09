package kmql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.StringsCompleter;

/**
 * A {@link Completer} implementation for interactive kmql session.
 */
public class QueryCompleter implements Completer {
    private final StringsCompleter delegate;

    public static QueryCompleter from(Engine engine) {
        return new QueryCompleter(engine, TableRegistry.DEFAULT);
    }

    QueryCompleter(Engine engine, TableRegistry tableRegistry) {
        Set<String> candidates = new HashSet<>();

        for (Entry<String, Table> entry : tableRegistry) {
            candidates.add(entry.getKey().toLowerCase());
            candidates.addAll(engine.showColumns(entry.getValue())
                                    .stream()
                                    .map(String::toLowerCase)
                                    .collect(Collectors.toSet()));
        }

        candidates.addAll(staticCandidates());

        delegate = new StringsCompleter(candidates);
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        delegate.complete(reader, line, candidates);
    }

    private static List<String> staticCandidates() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        QueryCompleter.class.getClassLoader()
                                            .getResourceAsStream("completion.txt"))
        )) {
            return reader.lines().collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
