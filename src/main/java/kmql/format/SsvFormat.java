package kmql.format;

/**
 * Space-Separated Values format.
 * Example:
 * {@code
 * # HEADER1, HEADER2, HEADER3
 * foo 1234 true
 * bar 5678 false
 * }
 */
public class  SsvFormat extends AbstractCsvFormat {
    public SsvFormat() {
        super(" ");
    }
}
