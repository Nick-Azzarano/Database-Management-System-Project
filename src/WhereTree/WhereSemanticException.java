package WhereTree;

public class WhereSemanticException extends RuntimeException {
    public WhereSemanticException(String message) {
        super("VALIDATION ERROR: " + message);
    }
}
