package Classes;

import java.util.ArrayList;

public class AttrSchema {
    private String name;
    private DataType type;
    private int length;
    private ArrayList<Constraint> constraints;

    public AttrSchema(String name, DataType type, int length) {
        this.name = name;
        this.type = type;
        this.length = length;
        this.constraints = new ArrayList<Constraint>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getLength() {
        return length;
    }

    public void addConstraint(Constraint constraint) {
        constraints.add(constraint);
    }

    public ArrayList<Constraint> getConstraints() {
        return constraints;
    }

    public void setConstraints(ArrayList<Constraint> constraints) {
        this.constraints = constraints;
    }

    public void removeConstraint() {
        this.constraints = new ArrayList<Constraint>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("--Name: ").append(name).append("\n");
        sb.append("---Data type: ").append(type).append("\n");
        if (type == DataType.CHAR || type == DataType.VARCHAR) {
            sb.append("---Max length: ").append(getLength()).append("\n");
        }
        if (!constraints.isEmpty()) {
            sb.append("---Constraints: ").append("\n");
            for (Constraint constraint : constraints) {
                sb.append("----").append(constraint.toString()).append("\n");
            }
        }
        return sb.toString();
    }
}