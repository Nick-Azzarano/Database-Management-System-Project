package WhereTree;

public class WhereSyntaxException extends RuntimeException {
    public WhereSyntaxException(String message) {
        super("PARSING ERROR: " + message);
    }
}
