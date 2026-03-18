package Classes;

import org.w3c.dom.Attr;

import java.util.ArrayList;

public class Page {
    private int tableId;
    private int pageNumber;
    private int numRecords;
    private boolean modified;
    private ArrayList<Record> records;

    public Page(int tableId, int pageNumber) {
        this.tableId = tableId;
        this.pageNumber = pageNumber;
        this.modified = false;
        this.records = new ArrayList<>();
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(int pageNumber) {
        this.pageNumber = pageNumber;
    }

    public int getNumRecords() {
        return numRecords;
    }

    public void setNumRecords(int numRecords) {
        this.numRecords = numRecords;
    }

    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    public ArrayList<Record> getRecords() {
        return records;
    }

    public void setRecords(ArrayList<Record> records) {
        this.records = records;
        this.numRecords = records.size();
    }

    public boolean isOverfull(int pageSize, ArrayList<AttrSchema> attributes) {
        int freeSpace = pageSize - 4; //always less 4 bytes for the int number of records
        for (Record record : records) {
            freeSpace -= 4; //less 4 bytes for null bitmap, always
            for (int i = 0; i < attributes.size(); i++) {
                if (record.getValues().get(i) == null) {
                    continue;
                }
                switch (attributes.get(i).getType()) {
                    case INTEGER:
                        freeSpace -= 4;
                        break;
                    case DOUBLE:
                        freeSpace -= 8;
                        break;
                    case BOOLEAN:
                        freeSpace -= 1;
                        break;
                    case CHAR:
                        freeSpace -= attributes.get(i).getLength();
                        break;
                    case VARCHAR:
                        freeSpace -= 4; //less 4 bytes for length int before varchar
                        freeSpace -= ((String)record.getValues().get(i)).length();
                        break;
                }
            }
        }
        return (freeSpace < 0);
    }
}