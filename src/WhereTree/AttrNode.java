package WhereTree;

import Classes.DataType;
import Classes.AttrSchema;

import java.util.ArrayList;

public class AttrNode implements WhereTree {

    private String attrName;
    private Object value;
    private DataType dataType;
    private NodeType nodeType;

    public AttrNode(String attrName, NodeType nodeType) {
        this.attrName = attrName;
        this.value = null;
        this.nodeType = nodeType;
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    public Object getValue() {
        return value;
    }

    public void populateTree(ArrayList<AttrSchema> attributes, ArrayList<Object> values) {
        int i = -1;
        for (int j = 0; j < attributes.size(); j++) {
            if (attributes.get(j).getName().equals(attrName)) {
                i = j;
                break;
            }
        }
        this.value = values.get(i);
    }

    public void validateTree() { }

    public boolean evaluateTree() {
        return true;
    }

    @Override
    public String toString() {
        return attrName;
    }

}
