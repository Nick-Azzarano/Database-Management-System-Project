package WhereTree;

import Classes.AttrSchema;

import java.util.ArrayList;

public class LogicNode implements WhereTree {

    private WhereTree leftNode;
    private WhereTree rightNode;
    private NodeType nodeType;

    public void setLeftNode(WhereTree leftNode) {
        this.leftNode = leftNode;
    }

    public void setRightNode(WhereTree rightNode) {
        this.rightNode = rightNode;
    }

    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    public void populateTree(ArrayList<AttrSchema> attributes, ArrayList<Object> values) {
        leftNode.populateTree(attributes, values);
        rightNode.populateTree(attributes, values);
    }

    public void validateTree() throws WhereSemanticException {
        if ((leftNode instanceof LogicNode || leftNode instanceof CompNode)
        && (rightNode instanceof LogicNode || rightNode instanceof CompNode)) {
            leftNode.validateTree();
            rightNode.validateTree();
        } else {
            throw new WhereSemanticException("Invalid LogicNode");
        }
    }

    public boolean evaluateTree() {
        switch (nodeType) {
            case AND_OP:
                return (leftNode.evaluateTree() && rightNode.evaluateTree());
            case OR_OP:
                return (leftNode.evaluateTree() || rightNode.evaluateTree());
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        if (leftNode == null) {
            sb.append("null ");
        } else {
            sb.append(leftNode).append(" ");
        }
        if (rightNode == null) {
            sb.append("null ");
        } else {
            sb.append(rightNode).append(" ");
        }
        switch (nodeType) {
            case AND_OP -> sb.append("and");
            case OR_OP -> sb.append("or");
        }
        sb.append(")");
        return sb.toString();
    }

}
