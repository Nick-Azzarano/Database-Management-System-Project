package WhereTree;

import Classes.DataType;
import Classes.AttrSchema;

import java.util.ArrayList;

public class ValNode implements WhereTree {

    private Object value;
    private NodeType nodeType;

    public ValNode(Object value, NodeType nodeType) {
        this.value = value;
        this.nodeType = nodeType;
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    public Object getValue() {
        return value;
    }

    public void populateTree(ArrayList<AttrSchema> attributes, ArrayList<Object> values) { }

    public void validateTree() { }

    public boolean evaluateTree() {
        return true;
    }

    @Override
    public String toString() {
        return switch (nodeType) {
            case INTEGER -> ((Integer) value).toString();
            case DOUBLE -> ((Double) value).toString();
            case BOOLEAN -> ((Boolean) value).toString();
            case STRING -> ('"' + (String) value + '"');
            default -> null;
        };
    }

}
