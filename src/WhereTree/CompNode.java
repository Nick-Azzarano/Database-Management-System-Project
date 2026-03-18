package WhereTree;

import Classes.AttrSchema;

import java.util.ArrayList;

public class CompNode implements WhereTree {

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
        if (!(leftNode instanceof AttrNode)) {
            throw new WhereSemanticException("Invalid CompNode");
        }
        if (rightNode instanceof AttrNode) {
            if (leftNode.getNodeType() != rightNode.getNodeType()) {
                throw new WhereSemanticException("Invalid CompNode");
            }
        } else if (rightNode instanceof ValNode) {
            if (leftNode.getNodeType() != rightNode.getNodeType()) {
                throw new WhereSemanticException("Invalid CompNode");
            }
        } else {
            throw new WhereSemanticException("Invalid CompNode");
        }
    }

    public boolean evaluateTree() {
        Object rightValue;
        if (rightNode instanceof ValNode) {
            rightValue = ((ValNode) rightNode).getValue();
        } else if (rightNode instanceof AttrNode) {
            rightValue = ((AttrNode) rightNode).getValue();
        } else {
            throw new WhereSemanticException("Invalid CompNode");
        }
        if (rightValue == null) {
            throw new WhereSemanticException("Invalid CompNode");
        }
        switch (leftNode.getNodeType()) {
            case INTEGER:
                Integer leftInt = (Integer) ((AttrNode)leftNode).getValue();
                Integer rightInt = (Integer) (rightValue);
                return switch (nodeType) {
                    case L_OP -> leftInt < rightInt;
                    case LE_OP -> leftInt <= rightInt;
                    case G_OP -> leftInt > rightInt;
                    case GE_OP -> leftInt >= rightInt;
                    case EQ_OP -> leftInt.equals(rightInt);
                    case NE_OP -> !leftInt.equals(rightInt);
                    default -> false;
                };
            case DOUBLE:
                Double leftDouble = (Double) ((AttrNode)leftNode).getValue();
                Double rightDouble = (Double) (rightValue);
                return switch (nodeType) {
                    case L_OP -> leftDouble < rightDouble;
                    case LE_OP -> leftDouble <= rightDouble;
                    case G_OP -> leftDouble > rightDouble;
                    case GE_OP -> leftDouble >= rightDouble;
                    case EQ_OP -> leftDouble.equals(rightDouble);
                    case NE_OP -> !leftDouble.equals(rightDouble);
                    default -> false;
                };
            case BOOLEAN:
                Boolean leftBool = (Boolean) ((AttrNode)leftNode).getValue();
                Boolean rightBool = (Boolean) rightValue;
                return switch (nodeType) {
                    case EQ_OP -> leftBool.equals(rightBool);
                    case NE_OP -> !(leftBool.equals(rightBool));
                    default -> false;
                };
            case STRING:
                String leftString = (String) ((AttrNode)leftNode).getValue();
                String rightString = (String) rightValue;
                return switch (nodeType) {
                    case EQ_OP -> leftString.equals(rightString);
                    case NE_OP -> !(leftString.equals(rightString));
                    default -> false;
                };
            default:
                throw new WhereSemanticException("Invalid CompNode");
        }
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
            case L_OP -> sb.append("<");
            case LE_OP -> sb.append("<=");
            case G_OP -> sb.append(">");
            case GE_OP -> sb.append(">=");
            case EQ_OP -> sb.append("=");
            case NE_OP -> sb.append("!=");
        }
        sb.append(")");
        return sb.toString();
    }

}
