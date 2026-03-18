package WhereTree;

import Classes.AttrSchema;

import java.util.ArrayList;

public interface WhereTree {

    public NodeType getNodeType();

    public void populateTree(ArrayList<AttrSchema> attributes, ArrayList<Object> values);

    public void validateTree() throws WhereSemanticException;

    public boolean evaluateTree();

}
