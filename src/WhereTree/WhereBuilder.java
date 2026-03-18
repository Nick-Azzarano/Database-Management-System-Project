package WhereTree;

import Classes.AttrSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;

public class WhereBuilder {

    private final String[] compareKeywords = {"<", "<=", ">", ">=", "=", "!="};
    private final String[] logicKeywords = {"and", "or"};
    private final String[] boolKeywords = {"true", "false"};

    private Vector<WhereTree> operandStack = new Vector<>();
    private Vector<WhereTree> compareStack = new Vector<>();
    private Vector<WhereTree> logicStack = new Vector<>();

    private final ArrayList<AttrSchema> attributes;
    private final String[] attributeNames;
    private WhereTree parseTree;


    public WhereBuilder(ArrayList<AttrSchema> attributes) {
        this.attributes = attributes;
        this.parseTree = null;
        attributeNames = new String[attributes.size()];
        for (int i = 0; i < attributes.size(); i++) {
            attributeNames[i] = attributes.get(i).getName();
        }
    }

    private void resolveCompareStack() {
        while (operandStack.size() >= 2 && !compareStack.isEmpty()) {
            if (compareStack.getLast() instanceof CompNode) {
                if ((operandStack.get(operandStack.size() - 1) instanceof AttrNode || operandStack.get(operandStack.size() - 1) instanceof ValNode)
                        && (operandStack.get(operandStack.size() - 2) instanceof AttrNode || operandStack.get(operandStack.size() - 2) instanceof ValNode)) {
                    WhereTree rightNode = operandStack.removeLast();
                    WhereTree leftNode = operandStack.removeLast();
                    CompNode newNode = (CompNode) compareStack.removeLast();
                    newNode.setLeftNode(leftNode);
                    newNode.setRightNode(rightNode);
                    operandStack.addFirst(newNode);
                    continue;
                }
            } else {
                throw new WhereSyntaxException("Unrecognized operator \"" + compareStack.getLast() + "\"");
            }
            break;
        }
    }

    private void resolveLogicStack() {
        while (operandStack.size() >= 2 && !logicStack.isEmpty()) {
            if (logicStack.getLast() instanceof LogicNode) {
                if ((operandStack.get(operandStack.size() - 1) instanceof CompNode || operandStack.get(operandStack.size() - 1) instanceof LogicNode)
                        && (operandStack.get(operandStack.size() - 2) instanceof CompNode || operandStack.get(operandStack.size() - 2) instanceof LogicNode)) {
                    WhereTree rightNode = operandStack.removeLast();
                    WhereTree leftNode = operandStack.removeLast();
                    LogicNode newNode = (LogicNode) logicStack.removeLast();
                    newNode.setLeftNode(leftNode);
                    newNode.setRightNode(rightNode);
                    operandStack.addFirst(newNode);
                    continue;
                }
            } else {
                throw new WhereSyntaxException("Unrecognized operator \"" + logicStack.getLast() + "\"");
            }
            break;
        }
    }

    public void parseStatement(String statement) throws WhereSyntaxException {
        //System.out.println("DEBUG parsing statement: " + statement);
        operandStack.clear();
        compareStack.clear();
        logicStack.clear();
        String token;
        while (!statement.isEmpty()) {
            if (statement.startsWith("\"")) {
                if (statement.substring(1).contains("\"")) {
                    token = statement.substring(1, statement.substring(1).indexOf('"') + 1);
                    statement = statement.substring(statement.substring(1).indexOf('"') + 2).strip();

                    //System.out.println("string found");
                    ValNode newNode = new ValNode(token, NodeType.STRING);
                    operandStack.addLast(newNode);
                } else {
                    throw new WhereSyntaxException("No closing quotes for string literal");
                }
            } else {
                if (!statement.contains(" ")) {
                    token = statement;
                    statement = "";
                } else {
                    token = statement.substring(0, statement.indexOf(' '));
                    statement = statement.substring(statement.indexOf(' ') + 1).strip();
                }
                if (Arrays.asList(compareKeywords).contains(token)) {
                    //System.out.println("compare keyword found");
                    CompNode newNode = new CompNode();
                    switch (token) {
                        case "<":
                            newNode.setNodeType(NodeType.L_OP);
                            break;
                        case "<=":
                            newNode.setNodeType(NodeType.LE_OP);
                            break;
                        case ">":
                            newNode.setNodeType(NodeType.G_OP);
                            break;
                        case ">=":
                            newNode.setNodeType(NodeType.GE_OP);
                            break;
                        case "=":
                            newNode.setNodeType(NodeType.EQ_OP);
                            break;
                        case "!=":
                            newNode.setNodeType(NodeType.NE_OP);
                    }
                    compareStack.addLast(newNode);
                } else if (Arrays.asList(logicKeywords).contains(token)) {
                    //System.out.println("logic keyword found");
                    LogicNode newNode = new LogicNode();
                    switch (token) {
                        case "and":
                            newNode.setNodeType(NodeType.AND_OP);
                            logicStack.addLast(newNode);
                            break;
                        case "or":
                            newNode.setNodeType(NodeType.OR_OP);
                            logicStack.addFirst(newNode);
                    }
                } else if (Arrays.asList(boolKeywords).contains(token)) {
                    //System.out.println("bool keyword found");
                    ValNode newNode = switch (token) {
                        case "true" -> new ValNode(true, NodeType.BOOLEAN);
                        case "false" -> new ValNode(false, NodeType.BOOLEAN);
                        default -> null;
                    };
                    operandStack.addLast(newNode);
                } else if (Arrays.asList(attributeNames).contains(token)) {
                    //System.out.println("attribute found");
                    int i = Arrays.asList(attributeNames).indexOf(token);
                    NodeType newNodeType = switch (attributes.get(i).getType()) {
                        case INTEGER -> NodeType.INTEGER;
                        case DOUBLE -> NodeType.DOUBLE;
                        case BOOLEAN -> NodeType.BOOLEAN;
                        case CHAR, VARCHAR -> NodeType.STRING;
                    };
                    AttrNode newNode = new AttrNode(token, newNodeType);
                    operandStack.addLast(newNode);
                } else {
                    if (token.contains(".")) {
                        try {
                            Double d = Double.parseDouble(token);
                            //System.out.println("number found");
                            ValNode newNode = new ValNode(d, NodeType.DOUBLE);
                            operandStack.addLast(newNode);
                        } catch (NumberFormatException e) {
                            throw new WhereSyntaxException("Unrecognized token \"" + token + "\"");
                        }
                    } else {
                        try {
                            Integer i = Integer.parseInt(token);
                            //System.out.println("number found");
                            ValNode newNode = new ValNode(i, NodeType.INTEGER);
                            operandStack.addLast(newNode);
                        } catch (NumberFormatException e) {
                            throw new WhereSyntaxException("Unrecognized token \"" + token + "\"");
                        }
                    }
                }
            }
            //System.out.println("DEBUG token: " + token + "\n");

            // Now check stacks to see if there's anything you can merge
            resolveCompareStack();
        }

        resolveCompareStack();

        resolveLogicStack();

        if (operandStack.size() == 1) {
            this.parseTree = operandStack.removeLast();
            try {
                this.parseTree.validateTree();
            } catch (WhereSemanticException e) {
                this.parseTree = null;
                throw e;
            }
        } else {
            throw new WhereSyntaxException("Couldn't evaluate complete parse tree");
        }
    }

    public boolean parseRecord(ArrayList<Object> values) {
        if (parseTree != null) {
            parseTree.populateTree(attributes, values);
            return (parseTree.evaluateTree());
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Attributes: \n");
        for (AttrSchema a : attributes) {
            sb.append(a.toString()).append("\n");
        }
        sb.append("Parse tree: \n").append(parseTree);
        return sb.toString();
    }

}
