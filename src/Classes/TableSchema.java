package Classes;

import java.util.ArrayList;

public class TableSchema {
    private String name;
    private int id;
    private int numPages;
    private int numRecords;
    private ArrayList<Integer> pageOrdering; 
    private ArrayList<AttrSchema> attributes;

    public TableSchema(String name) {
        this.name = name;
        this.id = 0;
        this.numPages = 0;
        this.numRecords = 0;
        this.pageOrdering = new ArrayList<Integer>();
        this.attributes = new ArrayList<AttrSchema>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getNumPages() {
        return numPages;
    }

    public void setNumPages(int numPages) {
        this.numPages = numPages;
    }

    public int getNumRecords() {
        return numRecords;
    }

    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }

    public ArrayList<Integer> getPageOrdering() {
        return pageOrdering;
    }

    public void setPageOrdering(ArrayList<Integer> pageOrdering) {
        this.pageOrdering = pageOrdering;
    }

    public int getPagebyIndex(int index) {
        if (index >= 0 && index < pageOrdering.size()) {
            return pageOrdering.get(index);
        }
        else {
            System.out.println("no page with index " + index + " in table " + name);
            return -1;
        }
    }

    public int getPrimaryIndex() {
        int i = 0;
        for (AttrSchema attr : attributes) {
            if (attr.getConstraints().contains(Constraint.PRIMARYKEY)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    public ArrayList<Integer> getUniqueIndex() {
        ArrayList<Integer> returnList = new ArrayList<>();
        int i = 0;
        for (AttrSchema attr : attributes) {
            if (attr.getConstraints().contains(Constraint.UNIQUE)) {
                returnList.add(i);
            }
            i++;
        }
        return returnList;
    }

    public ArrayList<AttrSchema> getAttributes() {
        return attributes;
    }

    public void setAttributes(ArrayList<AttrSchema> attributes) {
        this.attributes = attributes;
    }

    public void addAttrSchema(AttrSchema attrSchema) {
        attributes.add(attrSchema);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Table: ").append(name.toUpperCase()).append("\n");
        //sb.append("DEBUG table id: ").append(id).append("\n");
        sb.append("-").append(numPages).append(" page(s)\n");
        sb.append("-").append(numRecords).append(" record(s)\n");
        // sb.append("-").append("TABLE ID: ").append(id).append("\n");
        if (!attributes.isEmpty()) {
            sb.append("-Attributes:\n");
            for (AttrSchema attr : attributes) {
                sb.append(attr.toString());
            }
        }
        return sb.toString();
    }
}

    
