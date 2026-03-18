package Classes;

import WhereTree.WhereSemanticException;
import WhereTree.WhereSyntaxException;
import org.w3c.dom.Attr;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import javax.xml.crypto.Data;

import WhereTree.WhereBuilder;

public class StorageManager {
    private final String[] keywords = {"create", "drop", "add", "table", "default", "notnull", "primarykey", "unique", "insert", "into", "values", "from", "where", "orderby"};

    //buffer is an ArrayList of pages, first elem is most recently used, last is least recently used
    private ArrayList<Page> buffer = new ArrayList<>();

    private Catalog catalog;
    private String dbLocation;
    private int pageSize;
    private int bufferSize;

    public StorageManager(String dbLocation, int pageSize, int bufferSize) {
        this.catalog = null;
        this.dbLocation = dbLocation;
        this.pageSize = pageSize;
        this.bufferSize = bufferSize;
    }

    public void createDatabase() {
        this.catalog = new Catalog(0, pageSize, bufferSize);
        String catalogLocation = dbLocation + File.separator + "catalog";
        writeCatalog(catalogLocation, this.catalog);
        System.out.println("Creating database at " + dbLocation + File.separator);
        System.out.println("Starting database at " + dbLocation + File.separator + " with page size of " + catalog.getPageSize() + " byte(s) and buffer size of " + catalog.getBufferSize() + " page(s)");
    }

    public void restartDatabase() {
        String catalogLocation = dbLocation + File.separator + "catalog";
        this.catalog = readCatalog(catalogLocation);
        this.catalog.setBufferSize(bufferSize);
        writeCatalog(catalogLocation, this.catalog);
        System.out.println("Restarting database at " + dbLocation + File.separator + " with page size of " + catalog.getPageSize() + " byte(s) and buffer size of " + catalog.getBufferSize() + " page(s)");
    }

    public void flushPageBuffer() {
        for (Page page : buffer) {
            if (page.isModified()) {
                if (page.getTableId() < 0) {
                    //this is a temp table, so we don't write it to file
                    continue;
                }
                writePageToFile(catalog.getTableFromId(page.getTableId()).getName(), page.getPageNumber(), page);
            }
        }
    }

    public void trimPageBuffer() {
        if (buffer.size() > bufferSize) {
            Page page = buffer.removeLast();
            if (page.isModified()) {
                writePageToFile(catalog.getTableFromId(page.getTableId()).getName(), page.getPageNumber(), page);
            }
        }
    }

    public void dropTableFromBuffer(int tableId) {
        for (int i = buffer.size() - 1; i >= 0; i--) {
            if (buffer.get(i).getTableId() == tableId) {
                buffer.remove(i);
            }
        }
    }

    public void writePageToBuffer (Page page) {
        page.setModified(true);
        for (Page p : buffer) {
            if (p.getTableId() == page.getTableId() && p.getPageNumber() == page.getPageNumber()) {
                buffer.remove(p);
                buffer.add(0, page);
                trimPageBuffer();
                return;
            }
        }
        buffer.add(0, page);
        trimPageBuffer();
    }

    public Page getPageFromBuffer(int tableId, int pageNumber) {
        for (Page p : buffer) {
            if (p.getTableId() == tableId && p.getPageNumber() == pageNumber) {
                buffer.remove(p);
                buffer.add(0, p);
                trimPageBuffer();
                return p;
            }
        }
        Page page = getPageFromFile(catalog.getTableFromId(tableId).getName(), pageNumber);
        buffer.add(0, page);
        trimPageBuffer();
        return page;
    }

    public Page getPageFromFile(String tableName, int pageNumber) {
        TableSchema table = catalog.getTableFromName(tableName);
        Page page = new Page(catalog.getIdFromName(tableName), pageNumber);
        ArrayList<Record> records = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(dbLocation + File.separator + tableName + ".bin")) {
            long bytesToSkip = 4 + (long) pageNumber * pageSize;
            long n = fis.skip(bytesToSkip);
            if (n != bytesToSkip) {
                System.out.println("Error: Page file too short");
                return null;
            }

            byte[] intBytes = new byte[4];
            fis.read(intBytes);
            int numEntries = ByteBuffer.wrap(intBytes).getInt();
            ArrayList<AttrSchema> attributes = table.getAttributes();
            for (int i = 0; i < numEntries; i++) {
                Record newRecord = new Record("blah");
                fis.read(intBytes);
                int nullBitmap = ByteBuffer.wrap(intBytes).getInt();
                newRecord.setNullBitmap(nullBitmap);
                boolean[] nullBitmapArray = new boolean[attributes.size()];
                for (int j = 0; j < attributes.size(); j++) {
                    nullBitmap = nullBitmap >> 1;
                    nullBitmapArray[attributes.size() - j - 1] = (nullBitmap & 1) == 1;
                }
                //Now nullBitmapArray is an array of bools that tell us if a value will be null
                for (int j = 0; j < attributes.size(); j++) {
                    if (!nullBitmapArray[j]) { //if we expect to see a value for this attribute
                        switch (attributes.get(j).getType()) {
                            case INTEGER:
                                fis.read(intBytes);
                                newRecord.addValue(ByteBuffer.wrap(intBytes).getInt());
                                break;
                            case DOUBLE:
                                byte[] doubleBytes = new byte[8];
                                fis.read(doubleBytes);
                                newRecord.addValue(ByteBuffer.wrap(doubleBytes).getDouble());
                                break;
                            case BOOLEAN:
                                byte[] booleanByte = new byte[1];
                                fis.read(booleanByte);
                                if (ByteBuffer.wrap(booleanByte).getInt() == 1) {
                                    newRecord.addValue("true");
                                } else {
                                    newRecord.addValue("false");
                                }
                                break;
                            case CHAR:
                                byte[] charBytes = new byte[attributes.get(j).getLength()];
                                fis.read(charBytes);
                                String charString = new String(charBytes);
                                if (charString.contains("\0")) {
                                    charString = charString.substring(0, charString.indexOf('\0'));
                                }
                                newRecord.addValue(charString);
                                break;
                            case VARCHAR:
                                fis.read(intBytes);
                                int varcharLength = ByteBuffer.wrap(intBytes).getInt();
                                byte[] strBytes = new byte[varcharLength];
                                fis.read(strBytes);
                                newRecord.addValue(new String(strBytes));
                                break;
                        }
                    } else {
                        newRecord.addValue(null); //this way we can always index into each record the same way, even if some vals null
                    }
                }
                records.add(newRecord);
            }
            page.setRecords(records);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return page;
    }

    public void writePageToFile (String tableName, int pageId, Page page) {
        try (RandomAccessFile raf = new RandomAccessFile(dbLocation + File.separator + tableName + ".bin", "rw")) {
            byte[] bytes;
            TableSchema table = catalog.getTableFromName(tableName);
            raf.seek(0);
            bytes = ByteBuffer.allocate(4).putInt(table.getNumPages()).array();
            raf.write(bytes);
            raf.seek(4 + (long) pageId * pageSize);
            int freeSpace = pageSize;
            ArrayList<AttrSchema> attributes = table.getAttributes();

            bytes = ByteBuffer.allocate(4).putInt(page.getNumRecords()).array();
            raf.write(bytes);
            freeSpace -= 4;

            for (Record record : page.getRecords()) {
                bytes = ByteBuffer.allocate(4).putInt(record.getNullBitmap()).array();
                raf.write(bytes);
                freeSpace -= 4;
                for (int i = 0; i < record.getValues().size(); i++) {
                    Object value = record.getValues().get(i);
                    if (value == null) {
                        continue;
                    }
                    switch (attributes.get(i).getType()) {
                        case INTEGER:
                            bytes = ByteBuffer.allocate(4).putInt((Integer) value).array();
                            raf.write(bytes);
                            freeSpace -= 4;
                            break;
                        case DOUBLE:
                            bytes = ByteBuffer.allocate(8).putDouble((Double) value).array();
                            raf.write(bytes);
                            freeSpace -= 8;
                            break;
                        case BOOLEAN:
                            if (value.equals("true")) {
                                bytes = ByteBuffer.allocate(1).putInt(1).array();
                            } else {
                                bytes = ByteBuffer.allocate(1).putInt(0).array();
                            }
                            raf.write(bytes);
                            freeSpace -= 1;
                            break;
                        case CHAR:
                            bytes = value.toString().getBytes();
                            raf.write(bytes);
                            bytes = new byte[attributes.get(i).getLength() - value.toString().length()];
                            raf.write(bytes);
                            freeSpace -= attributes.get(i).getLength();
                            break;
                        case VARCHAR:
                            bytes = ByteBuffer.allocate(4).putInt(value.toString().length()).array();
                            raf.write(bytes);
                            freeSpace -= 4;
                            bytes = value.toString().getBytes();
                            raf.write(bytes);
                            freeSpace -= value.toString().length();
                            break;
                    }
                }
            }

            //write the free space left, defaults to 0
            bytes = new byte[freeSpace];
            raf.write(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void splitPage(Page pageToSplit) {
        TableSchema table = catalog.getTableFromId(pageToSplit.getTableId());
        int nextPageId = table.getNumPages();
        table.setNumPages(nextPageId + 1);
        catalog.dropTableName(table.getName());
        catalog.addExistingTable(table);
        writeCatalog(dbLocation + File.separator + "catalog", catalog);
        Page page1 = new Page(table.getId(), pageToSplit.getPageNumber());
        Page page2 = new Page(table.getId(), nextPageId);
        int halfIndex = pageToSplit.getNumRecords() / 2;
        ArrayList<Record> records1 = new ArrayList<>();
        ArrayList<Record> records2 = new ArrayList<>();
        for (int i = 0; i < halfIndex; i++) {
            records1.add(pageToSplit.getRecords().get(i));
        }
        for (int j = halfIndex; j < pageToSplit.getNumRecords(); j++) {
            records2.add(pageToSplit.getRecords().get(j));
        }
        page1.setRecords(records1);
        page2.setRecords(records2);
        ArrayList<Integer> pageOrdering = table.getPageOrdering();
        int pageIndex = pageOrdering.indexOf(pageToSplit.getPageNumber());
        pageOrdering.remove(pageIndex);
        pageOrdering.add(pageIndex, page1.getPageNumber());
        pageOrdering.add(pageIndex + 1, page2.getPageNumber());
        writePageToBuffer(page1);
        writePageToBuffer(page2);
    }


    public void renameTable(String oldName, String newName) {
        TableSchema table = catalog.getTableFromName(oldName);
        if (table == null) {
            return;
        }
        catalog.renameTable(oldName, newName);
        File oldFile = new File(dbLocation + File.separator + oldName + ".bin");
        File newFile = new File(dbLocation + File.separator + newName + ".bin");
        oldFile.renameTo(newFile);
        oldFile.delete();
    }

    public boolean insertRecordToTable(Record record, String tableName, boolean enforceConstraints, boolean endOfSelectWithCartesianProduct) {
        TableSchema table = catalog.getTableFromName(tableName);
        ArrayList<AttrSchema> attributes = table.getAttributes();
        ArrayList<Integer> pageOrdering = table.getPageOrdering();
        if (table.getNumRecords() == 0) {
            Page newPage = new Page(catalog.getIdFromName(tableName), 0);
            pageOrdering.add(0);
            table.setPageOrdering(pageOrdering);
            table.setNumPages(1);
            table.setNumRecords(1);
            catalog.dropTableName(tableName);
            catalog.addExistingTable(table);
            ArrayList<Record> records = new ArrayList<Record>();
            records.add(record);
            newPage.setRecords(records);
            newPage.setModified(true);

            int freeSpace = pageSize;

            try (FileOutputStream fos = new FileOutputStream(dbLocation + File.separator + tableName + ".bin")) {
                byte[] bytes;

                //write table and page info first
                bytes = ByteBuffer.allocate(4).putInt(table.getNumPages()).array();
                fos.write(bytes);
            } catch (Exception e) {
                e.printStackTrace();
            }
            writePageToBuffer(newPage);
            writeCatalog(dbLocation + File.separator + "catalog", catalog);
        } else {
            if (enforceConstraints) {
                //Go through all records to find duplicate unique attributes
                ArrayList<Integer> uniqueIndexes = table.getUniqueIndex();
                for (int pageId : pageOrdering) {
                    Page page = getPageFromBuffer(catalog.getIdFromName(tableName), pageId);
                    for (Record existingRecord : page.getRecords()) {
                        for (int i : uniqueIndexes) {
                            Object recordValue = record.getValues().get(i);
                            Object existingValue = existingRecord.getValues().get(i);
                            if (recordValue == null || existingValue == null) {
                                continue;
                            }
                            if (recordValue.equals(existingValue)) {
                                System.out.println("Error: Can't add record with duplicate unique attribute");
                                return false;
                            }
                        }
                    }
                }
            }

            //Start iterating through records
            int primaryIndex = table.getPrimaryIndex();
            if (endOfSelectWithCartesianProduct) {
                // if we are inserting records while at the end of a select query, we are here because
                // we are in the middle of dropping columns from the final temp table. When we drop a column
                // from a table, we technically just make a new one with the remaining columns and copy over the records.

                // We should only ever override and set primaryIndex to 0 when we have performed a cartesian product on multiple tables.
                // This is because the primary key may not exist in the resulting table after the cartesian product.
                primaryIndex = 0;
            }
            DataType primaryType = attributes.get(primaryIndex).getType();
            Object primaryValue = record.getValues().get(primaryIndex);
            boolean insertFound = false; //True when we find a spot where this record belongs before existing record
            for (int pageId : pageOrdering) {
                Page page = getPageFromBuffer(catalog.getIdFromName(tableName), pageId);
                int insertIndex = 0;
                for (Record existingRecord : page.getRecords()) {
                    Object existingValue = existingRecord.getValues().get(primaryIndex);
                    switch (primaryType) {
                        case INTEGER:
                            if ((Integer)primaryValue < (Integer)existingValue) {
                                insertFound = true;
                            } else if ((primaryValue).equals(existingValue) && enforceConstraints) {
                                //if we are enforcing constraints and the values are equal, it's a duplicate primary key
                                System.out.println("Error: Can't add record with duplicate primary attribute");
                                return false;
                            }
                            break;
                        case DOUBLE:
                            if ((Double)primaryValue < (Double)existingValue) {
                                insertFound = true;
                            } else if ((primaryValue).equals(existingValue) && enforceConstraints) {
                                //if we are enforcing constraints and the values are equal, it's a duplicate primary key
                                System.out.println("Error: Can't add record with duplicate primary attribute");
                                return false;
                            }
                            break;
                        case BOOLEAN: //bool, char, and varchar are all stored internally as strings, so they all become the same case
                        case CHAR:
                        case VARCHAR:
                            String primaryString = (String) primaryValue;
                            String existingString = (String) existingValue;
                            if (primaryString.compareTo(existingString) < 0) {
                                insertFound = true;
                            } else if (primaryString.compareTo(existingString) == 0 && enforceConstraints) {
                                //if we are enforcing constraints and the values are equal, it's a duplicate primary key
                                System.out.println("Error: Can't add record with duplicate primary attribute");
                                return false;
                            }
                            break;
                    }
                    if (insertFound) {
                        ArrayList<Record> newRecords = page.getRecords();
                        newRecords.add(insertIndex, record);
                        page.setRecords(newRecords);
                        int numRecords = table.getNumRecords();
                        table.setNumRecords(numRecords + 1);
                        catalog.dropTableName(tableName);
                        catalog.addExistingTable(table);
                        writeCatalog(dbLocation + File.separator + "catalog", catalog);
                        //check for page overfull
                        if (page.isOverfull(pageSize, attributes)) {
                            splitPage(page);
                            return true;
                        }
                        writePageToBuffer(page);
                        return true;
                    }
                    insertIndex++;
                }
                if (pageId == pageOrdering.getLast()) {
                    ArrayList<Record> newRecords = page.getRecords();
                    newRecords.add(record);
                    page.setRecords(newRecords);
                    int numRecords = table.getNumRecords();
                    table.setNumRecords(numRecords + 1);
                    catalog.dropTableName(tableName);
                    catalog.addExistingTable(table);
                    writeCatalog(dbLocation + File.separator + "catalog", catalog);
                    //check for page overfull
                    if (page.isOverfull(pageSize, attributes)) {
                        splitPage(page);
                        return true;
                    }
                    writePageToBuffer(page);
                    return true;
                }
            }
        }
        return true;
    }

    public void writeCatalog(String catalogLocation, Catalog catalog) {
        try (FileOutputStream fos = new FileOutputStream(catalogLocation)) {
            byte[] bytes;

            //write nextId
            bytes = ByteBuffer.allocate(4).putInt(catalog.getNextId()).array();
            fos.write(bytes);

            //write num tables
            bytes = ByteBuffer.allocate(4).putInt(catalog.getNumTables()).array();
            fos.write(bytes);

            //write page size
            bytes = ByteBuffer.allocate(4).putInt(catalog.getPageSize()).array();
            fos.write(bytes);

            //write buffer size
            bytes = ByteBuffer.allocate(4).putInt(catalog.getBufferSize()).array();
            fos.write(bytes);

            //for each table in catalog
            for (TableSchema table : catalog.getTables()) {
                //write num attributes
                bytes = ByteBuffer.allocate(4).putInt(table.getAttributes().size()).array();
                fos.write(bytes);

                //write name, first length then the actual string
                bytes = table.getName().getBytes();
                fos.write(ByteBuffer.allocate(4).putInt(bytes.length).array());
                fos.write(bytes);

                //write id
                bytes = ByteBuffer.allocate(4).putInt(table.getId()).array();
                fos.write(bytes);

                //write numPages
                bytes = ByteBuffer.allocate(4).putInt(table.getNumPages()).array();
                fos.write(bytes);

                //write numRecords
                bytes = ByteBuffer.allocate(4).putInt(table.getNumRecords()).array();
                fos.write(bytes);

                //write pageOrdering, first length then actual list
                bytes = ByteBuffer.allocate(4).putInt(table.getPageOrdering().size()).array();
                fos.write(bytes);
                for (Integer i : table.getPageOrdering()) {
                    bytes = ByteBuffer.allocate(4).putInt(i).array();
                    fos.write(bytes);
                }

                for (AttrSchema attr : table.getAttributes()) {
                    //write name, first length then the actual string
                    bytes = attr.getName().getBytes();
                    fos.write(ByteBuffer.allocate(4).putInt(bytes.length).array());
                    fos.write(bytes);

                    //write datatype
                    bytes = ByteBuffer.allocate(4).putInt(attr.getType().ordinal()).array();
                    fos.write(bytes);

                    //write length
                    bytes = ByteBuffer.allocate(4).putInt(attr.getLength()).array();
                    fos.write(bytes);

                    //write constraints, first length then constraints
                    bytes = ByteBuffer.allocate(4).putInt(attr.getConstraints().size()).array();
                    fos.write(bytes);
                    for (Constraint constraint : attr.getConstraints()) {
                        bytes = ByteBuffer.allocate(4).putInt(constraint.ordinal()).array();
                        fos.write(bytes);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Catalog readCatalog(String catalogLocation) {
        Catalog returnCatalog = new Catalog(0, 0, 0);
        try (FileInputStream fis = new FileInputStream(catalogLocation)) {
            byte[] intBytes = new byte[4];

            //read nextId
            fis.read(intBytes);
            returnCatalog.setNextId(ByteBuffer.wrap(intBytes).getInt());

            //read numtables
            fis.read(intBytes);
            int numTables = ByteBuffer.wrap(intBytes).getInt();

            //read page size
            fis.read(intBytes);
            returnCatalog.setPageSize(ByteBuffer.wrap(intBytes).getInt());

            //read buffer size
            fis.read(intBytes);
            returnCatalog.setBufferSize(ByteBuffer.wrap(intBytes).getInt());

            ArrayList<TableSchema> tables = new ArrayList<TableSchema>();

            //for each table
            for (int i = 0; i < numTables; i++) {
                TableSchema newTable = new TableSchema("");

                //reading num attributes
                fis.read(intBytes);
                int numAttributes = ByteBuffer.wrap(intBytes).getInt();

                //read name, first length then actual string
                fis.read(intBytes);
                int nameLength = ByteBuffer.wrap(intBytes).getInt();
                byte[] nameBytes = new byte[nameLength];
                fis.read(nameBytes);
                newTable.setName(new String(nameBytes));

                //read id
                fis.read(intBytes);
                newTable.setId(ByteBuffer.wrap(intBytes).getInt());

                //read numpages
                fis.read(intBytes);
                newTable.setNumPages(ByteBuffer.wrap(intBytes).getInt());

                //read numrecords
                fis.read(intBytes);
                newTable.setNumRecords(ByteBuffer.wrap(intBytes).getInt());

                //read page ordering, first length then actual list
                ArrayList<Integer> newPageOrdering = new ArrayList<Integer>();
                fis.read(intBytes);
                int pageOrderingLength = ByteBuffer.wrap(intBytes).getInt();
                for (int j = 0; j < pageOrderingLength; j++) {
                    fis.read(intBytes);
                    newPageOrdering.add(ByteBuffer.wrap(intBytes).getInt());
                }
                newTable.setPageOrdering(newPageOrdering);

                //go through attributes
                for (int j = 0; j < numAttributes; j++) {
                    AttrSchema newAttr = new AttrSchema("", DataType.INTEGER, 0);

                    //read name, first length then string
                    fis.read(intBytes);
                    nameLength = ByteBuffer.wrap(intBytes).getInt();
                    nameBytes = new byte[nameLength];
                    fis.read(nameBytes);
                    String attrName = new String(nameBytes);
                    newAttr.setName(attrName);

                    //read datatype by reading int then mapping to enum
                    fis.read(intBytes);
                    int ordinal = ByteBuffer.wrap(intBytes).getInt();
                    newAttr.setType(DataType.values()[ordinal]);

                    //read length
                    fis.read(intBytes);
                    newAttr.setLength(ByteBuffer.wrap(intBytes).getInt());

                    //get constraints
                    fis.read(intBytes);
                    int constraintCount = ByteBuffer.wrap(intBytes).getInt();
                    ArrayList<Constraint> constraints = new ArrayList<Constraint>();
                    for (int k = 0; k < constraintCount; k++) {
                        fis.read(intBytes);
                        ordinal = ByteBuffer.wrap(intBytes).getInt();
                        constraints.add(Constraint.values()[ordinal]);
                    }
                    newAttr.setConstraints(constraints);

                    newTable.addAttrSchema(newAttr);
                }

                returnCatalog.addExistingTable(newTable);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnCatalog;
    }

    public String getDbLocation() {
        return dbLocation;
    }

    public void setDbLocation(String dbLocation) {
        this.dbLocation = dbLocation;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void parseCreateTable(String command) {
        command = command.substring("create table ".length()).trim();
        String tableName = command.substring(0, command.indexOf("(")).trim();
        System.out.println("Creating table \"" + tableName + "\"...");
        for (TableSchema table : catalog.getTables()) {
            if (table.getName().equals(tableName)) {
                System.out.println("Error: A table with the name " + tableName + " already exists.");
                return;
            }
        }
        TableSchema newTable = new TableSchema(tableName);

        command = command.substring(tableName.length()).trim();
        if (!command.substring(command.lastIndexOf(")")).equals(");")) {
            System.out.println("Error: Expected ; after )");
            return;
        }
        command = command.substring(1, command.lastIndexOf(")")).trim();
        String[] attributeStrings = command.split(",");
        if (attributeStrings.length == 0) {
            System.out.println("Error: No attributes given");
            return;
        }
        ArrayList<AttrSchema> attributes = new ArrayList<>(attributeStrings.length);
        int primaryCount = 0;
        for (String s : attributeStrings) {
            s = s.trim();
            //System.out.println("Attr token: " + s);
            String[] attributeParts = s.split("\\s+");
            if (attributeParts.length < 2 || attributeParts.length > 4) {
                System.out.println("Error: Invalid attribute token");
                return;
            }
            String newName = attributeParts[0].trim();
            for (AttrSchema a : attributes) {
                if (a.getName().equals(newName)) {
                    System.out.println("Error: Can't have attributes with duplicate names");
                    return;
                }
            }
            DataType newType;
            int length = 0;
            if (attributeParts[1].equals("integer")) {
                newType = DataType.INTEGER;
            } else if (attributeParts[1].equals("double")) {
                newType = DataType.DOUBLE;
            } else if (attributeParts[1].equals("boolean")) {
                newType = DataType.BOOLEAN;
            } else if (attributeParts[1].startsWith("char(")) {
                newType = DataType.CHAR;
                if (!attributeParts[1].contains(")")) {
                    System.out.println("Error: Expected length after char(");
                    return;
                }
                try {
                    length = Integer.parseInt(attributeParts[1].substring("char(".length(), attributeParts[1].length() - 1));
                } catch (NumberFormatException e) {
                    System.out.println("Error: Given length is not an integer");
                    return;
                }
            } else if (attributeParts[1].startsWith("varchar(")) {
                newType = DataType.VARCHAR;
                if (!attributeParts[1].contains(")")) {
                    System.out.println("Error: Expected length after varchar(");
                    return;
                }
                try {
                    length = Integer.parseInt(attributeParts[1].substring("varchar(".length(), attributeParts[1].length() - 1));
                } catch (NumberFormatException e) {
                    System.out.println("Error: Given length is not an integer");
                    return;
                }
            } else {
                System.out.println("Error: Invalid data type \"" + attributeParts[1] + "\"");
                return;
            }

            AttrSchema newAttr = new AttrSchema(newName, newType, length);

            if (attributeParts.length >= 3) {
                if (attributeParts[2].equals("primarykey")) {
                    newAttr.addConstraint(Constraint.PRIMARYKEY);
                    primaryCount++;
                    if (attributeParts.length == 4) {
                        System.out.println("Error: Second constraint after primarykey is redundant");
                        return;
                    }
                } else if (attributeParts[2].equals("unique")) {
                    newAttr.addConstraint(Constraint.UNIQUE);
                } else if (attributeParts[2].equals("notnull")) {
                    newAttr.addConstraint(Constraint.NOTNULL);
                } else {
                    System.out.println("Error: Invalid constraint given");
                    return;
                }
            }
            if (attributeParts.length == 4) {
                if (attributeParts[3].equals("primarykey")) {
                    System.out.println("Error: primarykey as second constraint is redundant");
                    return;
                } else if (attributeParts[3].equals("unique")) {
                    if (newAttr.getConstraints().contains(Constraint.UNIQUE)) {
                        System.out.println("Error: Attribute already has unique constraint");
                        return;
                    }
                    newAttr.addConstraint(Constraint.UNIQUE);
                } else if (attributeParts[3].equals("notnull")) {
                    if (newAttr.getConstraints().contains(Constraint.NOTNULL)) {
                        System.out.println("Error: Attribute already has notnull constraint");
                        return;
                    }
                    newAttr.addConstraint(Constraint.NOTNULL);
                } else {
                    System.out.println("Error: Invalid constraint given");
                    return;
                }
            }

            attributes.add(newAttr);
        }

        if (primaryCount != 1) {
            System.out.println("Error: Exactly one attribute must be primary key");
            return;
        }
        newTable.setAttributes(attributes);

        /*command = command.substring(command.indexOf("(") + 1).trim();
        while (!command.startsWith(");")) {
            String a_name = command.substring(0, command.indexOf(" ")).trim();
            command = command.substring(command.indexOf(" ") + 1).trim().strip();
            String a_type = command.substring(0, command.indexOf(" ")).trim();
            command = command.substring(command.indexOf(" ") + 1).trim().strip();
            DataType type;
            int length = 0;
            if (a_type.toUpperCase().equals("INTEGER")) {
                type = DataType.INTEGER;
            }
            else if (a_type.toUpperCase().equals("DOUBLE")) {
                type = DataType.DOUBLE;
            }
            else if (a_type.toUpperCase().equals("BOOLEAN")) {
                type = DataType.BOOLEAN;
            }
            else if (a_type.toUpperCase().startsWith("CHAR(")) {
                type = DataType.CHAR;
                length = Integer.parseInt(a_type.substring(5, a_type.indexOf(")")));

            }
            else if (a_type.toUpperCase().startsWith("VARCHAR(")) {
                type = DataType.VARCHAR;
                length = Integer.parseInt(a_type.substring(8, a_type.indexOf(")")));
            }
            else {
                System.out.println("Error: Invalid data type %s.\\n");
                return;
            }
            ArrayList<Constraint> constraints = new ArrayList<Constraint>();
            if (command.startsWith("primarykey")) {
                constraints.add(Constraint.PRIMARYKEY);
                command = command.substring("PRIMARYKEY".length()).trim().strip();
            }
            else if (command.startsWith("notnull")) {
                constraints.add(Constraint.NOTNULL);
                command = command.substring("NOTNULL".length()).trim().strip();
                if (command.startsWith("unique")) {
                    constraints.add(Constraint.UNIQUE);
                    command = command.substring("UNIQUE".length()).trim().strip();
                }
            }
            else if (command.startsWith("unique")) {
                constraints.add(Constraint.UNIQUE);
                command = command.substring("UNIQUE".length()).trim().strip();
                if (command.startsWith("notnull")) {
                    constraints.add(Constraint.NOTNULL);
                    command = command.substring("NOTNULL".length()).trim().strip();
                }
            }
            AttrSchema newAttr = new AttrSchema(a_name, type, length);
            newAttr.setConstraints(constraints);
            if (command.startsWith(",")) {
                command = command.substring(1).trim().strip();
            }
            newTable.addAttrSchema(newAttr);
        }*/

        try (FileOutputStream fos = new FileOutputStream(dbLocation + File.separator + tableName + ".bin")) {
            byte[] bytes;

            bytes = ByteBuffer.allocate(4).putInt(0).array();
            fos.write(bytes);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        catalog.addTable(newTable);
        writeCatalog(dbLocation + File.separator + "catalog", catalog);
        System.out.println("Table " + tableName + " created successfully.");
    }

    public void parseDropTable(String command) {
        command = command.substring("drop table ".length()).trim();
        String tableName = command.substring(0, command.indexOf(";")).trim();
        System.out.println("Dropping table \"" + tableName + "\"...");
        for (TableSchema table : catalog.getTables()) {
            if (table.getName().equals(tableName)) {
                catalog.dropTableName(tableName);
                writeCatalog(dbLocation + File.separator + "catalog", catalog);
                File tableFile = new File(dbLocation + File.separator + tableName + ".bin");
                if (tableFile.exists()) {
                    tableFile.delete();
                }
                dropTableFromBuffer(table.getId());
                System.out.println("Table " + tableName + " dropped successfully.");
                return;
            }
        }
        System.out.println("Error: Table " + tableName + " not found");
    }

    public void displaySchema() {
        System.out.println("Displaying schema for catalog...");
        System.out.println("Database location: " + dbLocation + File.separator);
        System.out.println(catalog.toString());
    }

    public void parseDisplayTable(String command) {
        String tableName = command.substring("display info ".length()).trim();
        tableName = tableName.substring(0, tableName.length() - 1);
        System.out.println("Displaying info for table \"" + tableName + "\"...");
        for (TableSchema table : catalog.getTables()) {
            if (table.getName().equals(tableName)) {
                System.out.println(table.toString());
                return;
            }
        }
        System.out.println("Error: Table " + tableName + " not found");
    }

    public void parseSelect(String command) {
        System.out.println("Getting records...");
        command = command.substring("select ".length()).trim().strip();
        String nextstep = ";";
        if (command.contains("where")){
            nextstep = "where";
        }
        else if (command.contains("orderby")) {
            nextstep = "orderby";
        }
        if (command.startsWith("*")) {
        //region SELECT *
            command = command.substring("*".length()).trim().strip();
        //endregion

        //region FROM <table names>
        //endregion
            command = command.substring(command.indexOf("from") + 4).trim().strip();
            String[] tableNames = command.substring(0, command.indexOf(nextstep)).trim().split(",");
            for (int i = 0; i < tableNames.length; i++) { // Trim whitespace from each table name
                TableSchema table = catalog.getTableFromName(tableNames[i].trim());
                if (table == null) {
                    System.out.println("Error: Table \"" + tableNames[i].trim() + "\" not found.");
                    return;
                }
            }
            ArrayList<String> attributes = new ArrayList<>(); // This will hold the attributes to be selected, if any are specified in the select statement. For SELECT *, this will be empty.
            ArrayList<String> validAttributes = new ArrayList<>(); // This will hold the valid attribute names for checking ambiguity later on. This will be used to check if an attribute is ambiguous or not when using SELECT *.
            for(int i = 0; i < tableNames.length; i++) {
                tableNames[i] = tableNames[i].trim();
            }

            ArrayList<String> tableNamesList = new ArrayList<>();
            for (String name : tableNames) {
                tableNamesList.add(name);
                for (AttrSchema attr: catalog.getTableFromName(name).getAttributes()) { // For each attribute in the table, add it to the attributes list if SELECT * is used
                    attributes.add(attr.getName());
                    validAttributes.add(name + "." + attr.getName()); // This will be used later to check for ambiguity in the attribute names
                }
            }
            ArrayList<String> tableNamesListCopy = new ArrayList<>();
            for (String name : tableNamesList) {
                tableNamesListCopy.add(name);
            }
            String fromTable = cartesianProduct(tableNamesListCopy);
            // System.out.println("Temp table name: " + fromTable);
            if (nextstep.equals("orderby")) {
                String attr = command.substring(command.indexOf("orderby") + 8).trim().strip();
                attr = attr.substring(0, attr.indexOf(";")).trim().strip();
                if (tableNames.length == 1) {    
                    for (int i = 0; i < attributes.size(); i++) { // Check if the attribute being ordered by is in the valid attributes list
                        if (attr.equals(attributes.get(i))) {
                            attr = validAttributes.get(i); // Replace with the full attribute name
                            break;
                        }
                    }
                }
                if (!validAttributes.contains(attr)) {
                    // If the attribute specified in the order by clause is not valid, print an error and return
                    System.out.println("ERROR -- Attribute " + attr + " is not valid for ordering.");
                    return;
                }
                String orderedTableName = orderBYClause(attr, tableNamesList, fromTable);
                if (orderedTableName.equals("")) {
                    // There was an error inside of orderByClause, so we should not continue
                    return;
                }
                selectAll(orderedTableName);
                return;
            }
            else if (nextstep.equals("where")) {
                TableSchema wheretable = new TableSchema("whereTable"); // Get the table schema for the where clause processing
                wheretable.setAttributes(catalog.getTableFromName(fromTable).getAttributes()); // Set the attributes of the temporary table to match the original table's attributes
                wheretable.setId(catalog.getNextId());
                catalog.setNextId(catalog.getNextId() + 1);
                wheretable.setNumPages(0);
                wheretable.setNumRecords(0);
                wheretable.setPageOrdering(new ArrayList<>());

                // Add the temporary table to the catalog
                catalog.addTempTable(wheretable);
                writeCatalog(dbLocation + File.separator + "catalog", catalog);

                WhereBuilder wb = new WhereBuilder(catalog.getTableFromName(fromTable).getAttributes());
                String whereClause = command.substring(command.indexOf("where") + 5).trim().strip();

                if (command.contains("orderby")) {
                    whereClause = whereClause.substring(0, whereClause.indexOf("orderby")).trim().strip();
                }
                else {
                    whereClause = whereClause.substring(0, whereClause.indexOf(";")).trim().strip();
                }

                StringBuilder sb = new StringBuilder();
                for (String whereClauseToken: whereClause.split(" ")) { // Split the where clause into tokens to parse
                    int attributeIndex = -1;
                    for (int i = 0; i < attributes.size(); i++) { // Check if the token matches any of the attributes being selected
                        if (whereClauseToken.trim().equals(attributes.get(i))) {
                            if (attributeIndex == -1) {
                                attributeIndex = i; // Replace the token with the full attribute name (e.g., tableName.attributeName)
                            } else {
                                System.out.println("ERROR -- " + whereClauseToken + " is ambiguous.");
                                return;
                            }
                        }
                    }
                    if (attributeIndex != -1) {
                        whereClauseToken = validAttributes.get(attributeIndex);
                    }
                    sb.append(whereClauseToken).append(" "); // Append the token to the StringBuilder to form the full where clause for parsing
                }
                try {
                    wb.parseStatement(sb.toString());
                } catch (WhereSyntaxException | WhereSemanticException e) { // Handle any exceptions that occur during parsing of the where clause
                    System.out.println(e.getMessage());
                    return;
                }
                

                for (int pageNum: catalog.getTableFromName(fromTable).getPageOrdering()) { // Iterate through each page of the table to find records that match the where clause
                    Page page = getPageFromBuffer(catalog.getTableFromName(fromTable).getId(), pageNum); // Get the page from the buffer using the table ID and page number
                    for (Record record: page.getRecords()) {
                        if (wb.parseRecord(record.getValues())) {
                            insertRecordToTable(record, wheretable.getName(), false, false);
                        }
                    }
                }
                if (command.contains("orderby")) {
                    String attr = command.substring(command.indexOf("orderby") + 8).trim().strip();
                    attr = attr.substring(0, attr.indexOf(";")).trim().strip();
                    for (int i = 0; i < attributes.size(); i++) { // Check if the attribute being ordered by is in the valid attributes list
                        if (attr.equals(attributes.get(i))) {
                            attr = validAttributes.get(i); // Replace with the full attribute name
                            break;
                        }
                    }
                    if (!validAttributes.contains(attr)) {
                        // If the attribute specified in the order by clause is not valid, print an error and return
                        System.out.println("ERROR -- Attribute " + attr + " is not valid for ordering.");
                        return;
                    }
                    String orderedTableName = orderBYClause(attr, tableNamesList, wheretable.getName());
                    if (orderedTableName.equals("")) {
                        // There was an error inside of orderByClause, so we should not continue
                        return;
                    }
                    selectAll(orderedTableName);
                    return;
                } else {
                    selectAll(wheretable.getName()); // Select all records from the temporary table without ordering
                }
                return; // Return after processing the where clause to avoid further processing
            }
            //System.out.println(tableName);
            selectAll(fromTable);
        }
        else {
        //region SELECT <attributes>
            // Extract the attributes being selected
            String finalTableName;
            String attributesPart = command.substring(0, command.indexOf("from")).trim().strip();
            String[] attributes = attributesPart.split(",");
            for (int i = 0; i < attributes.length; i++) {
                attributes[i] = attributes[i].trim();
            }
        //endregion

        //region FROM <table_names>
            // Extract the table names
            command = command.substring(command.indexOf("from") + 4).trim();
            String[] tableNames = command.substring(0, command.indexOf(nextstep)).trim().split(",");
            for (int i = 0; i < tableNames.length; i++) { // Trim whitespace from each table name
                TableSchema table = catalog.getTableFromName(tableNames[i].trim());
                if (table == null) {
                    System.out.println("Error: Table \"" + tableNames[i].trim() + "\" not found.");
                    return;
                }
            }
            ArrayList<String> tableNamesList = new ArrayList<>();
            for (String name : tableNames) {
                tableNamesList.add(name.trim());
            }
            

            // Ensure that there is at least 1 table name given
            if (tableNamesList.size() == 0) {
                System.out.println("ERROR -- No tables given in the FROM clause.");
                return;
            }

            // Validate table names
            for (String tableName : tableNamesList) {
                if (catalog.getTableFromName(tableName) == null) {
                    System.out.println("ERROR -- No such table " + tableName + ".");
                    return;
                }
            }
            

            boolean isSingleTable = tableNamesList.size() == 1;
            ArrayList<String> validAttributes = new ArrayList<>();
            for (String attribute : attributes) {
                boolean isValid = false;
                boolean isAmbiguous = false;

                for (String tableName : tableNamesList) {
                    TableSchema table = catalog.getTableFromName(tableName);
                    for (AttrSchema attr : table.getAttributes()) {
                        String fullAttributeName = tableName + "." + attr.getName();

                        if (attribute.equals(attr.getName())) {
                            if (isSingleTable) {
                                isValid = true;
                                validAttributes.add(tableName + "." + attr.getName());
                            } else {
                                isAmbiguous = true;
                            }
                        } else if (attribute.equals(fullAttributeName)) {
                            isValid = true;
                            validAttributes.add(fullAttributeName);
                        }
                    }
                }

                if (isAmbiguous) {
                    System.out.println("ERROR -- " + attribute + " is ambiguous.");
                    return;
                }

                if (!isValid) {
                    System.out.println("ERROR -- There is no such attribute " + attribute + ".");
                    return;
                }
                
            }

            // This value is a name of a temp table with a NEGATIVE ID value
            // Perform the cartesian product of the tables
            ArrayList<String> tableNamesListCopy = new ArrayList<>();
            for (String name : tableNamesList) {
                tableNamesListCopy.add(name);
            }
            String fromTableNAME = cartesianProduct(tableNamesListCopy);
            finalTableName = fromTableNAME; // Store the final table name for further processing
            //System.out.println("Temp table name: " + fromTable);
            // System.out.println("Attributes being selected: " + validAttributes.toString());

        //endregion

        //region WHERE <conditions>

        if (nextstep.equals("where")) {
            TableSchema wheretable = new TableSchema("whereTable"); // Get the table schema for the where clause processing
            wheretable.setAttributes(catalog.getTableFromName(fromTableNAME).getAttributes()); // Set the attributes of the temporary table to match the original table's attributes
            wheretable.setId(catalog.getNextTempId());
            catalog.setNextTempId(catalog.getNextTempId() - 1);
            wheretable.setNumPages(0);
            wheretable.setNumRecords(0);
            wheretable.setPageOrdering(new ArrayList<>());

            // Add the temporary table to the catalog
            catalog.addTempTable(wheretable);
            writeCatalog(dbLocation + File.separator + "catalog", catalog);
            WhereBuilder wb = new WhereBuilder(catalog.getTableFromName(wheretable.getName()).getAttributes());

            String whereClause = command.substring(command.indexOf("where") + 5).trim().strip();
            if (command.contains("orderby")) {
                whereClause = whereClause.substring(0, whereClause.indexOf("orderby")).trim().strip();
            } else {
                whereClause = whereClause.substring(0, whereClause.indexOf(";")).trim().strip();
            }
            StringBuilder sb = new StringBuilder();
            for (String whereClauseToken: whereClause.split(" ")) { // Split the where clause into tokens to parse
                int attributeIndex = -1;
                for (int i = 0; i < attributes.length; i++) { // Check if the token matches any of the attributes being selected
                    if (whereClauseToken.trim().equals(attributes[i])) {
                        if (attributeIndex == -1) {
                            attributeIndex = i; // Replace the token with the full attribute name (e.g., tableName.attributeName)
                        } else {
                            System.out.println("ERROR -- " + whereClauseToken + " is ambiguous.");
                            return;
                        }
                    }
                }
                if (attributeIndex != -1) {
                    whereClauseToken = validAttributes.get(attributeIndex);
                }
                sb.append(whereClauseToken).append(" "); // Append the token to the StringBuilder to form the full where clause for parsing
            }
            try {
                wb.parseStatement(sb.toString());
            } catch (WhereSyntaxException | WhereSemanticException e) { // Handle any exceptions that occur during parsing of the where clause
                System.out.println(e.getMessage());
                return;
            }

            // Iterate through each page of the table to find records that match the where clause
            for (int pageNum : catalog.getTableFromName(finalTableName).getPageOrdering()) {
                Page page = getPageFromBuffer(catalog.getTableFromName(finalTableName).getId(), pageNum); // Get the page from the buffer using the table ID and page number
                for (Record record : page.getRecords()) {
                    if (wb.parseRecord(record.getValues())) {
                        insertRecordToTable(record, wheretable.getName(), false, false);
                    }
                }
            }
            finalTableName = wheretable.getName(); // Update the final table name to the temporary table created for the where clause processing
        }
        //endregion

        //region FINISHING SELECT LOGIC

            // Drop the columns that we are NOT selecting
            // This will be done by dropping the columns from the temporary table that we have after we've tried OrderBy
            dropAttributeForSelect(finalTableName, validAttributes);
            
        //endregion

        //region ORDERBY <attribute>

            // Check for ORDERBY clause and extract the attribute if present
            if (command.contains("orderby")) {
                String attr = command.substring(command.indexOf("orderby") + 8).trim().strip();
                attr = attr.substring(0, attr.indexOf(";")).trim().strip();
                for (int i = 0; i < attributes.length; i++) { // Check if the attribute being ordered by is in the valid attributes list
                    if (attr.equals(attributes[i])) {
                        attr = validAttributes.get(i); // Replace with the full attribute name
                        break;
                    }
                }
                if (!validAttributes.contains(attr)) {
                    // If the attribute specified in the order by clause is not valid, print an error and return
                    System.out.println("ERROR -- Attribute " + attr + " is not valid for ordering.");
                    return;
                }
                String orderedTableName = orderBYClause(attr, tableNamesList, finalTableName);
                if (orderedTableName.equals("")) {
                    // There was an error inside of orderByClause, so we should not continue
                    return;
                }
                finalTableName = orderedTableName; // Update the final table name to the ordered table name
            }

        //endregion

            // THE FINAL LEG OF THE SELECT QUERY
            //
            // Display the selected attributes
            // selectSpecificAttributes(fromTable, validAttributes);
            selectAll(finalTableName);
        }

            //GET NAMES
            //RECURSIVE FUNC
            // TAKE IN LIST OF TABLE NAMES
            // IF LIST ONLY HAS 1 RETURN THAT NAME OR COPY WHATEVER TEAM DECIDES
            // ELSE POP 2 NAMES
                // MAKE TEMP TABLE
                // ITERSTE TABLES
                    //JOIN TUPLES
                    //INSERT INTO NEW TABLE
                    // CHECK IF ATTR HAS A QUALIFIER

                    // R1APPEND(R2) INSTEAD OF JOIN
        
    }

    /***
     * Recursive function that takes in a list of table names and performs a cartesian product on them
     * @param tableNames names of the tables to perform the cartesian product on 
     * @return the name of the new table created from the cartesian product
     */
    public String cartesianProduct(ArrayList<String> tableNames)
    {

        // NEED TO WRITE ALL TABLES TO CATALOG AND THEN DROP THEM STILL

        // Base Case to check if there is only one argument
        // Useful with delete and update
        // Should handle those no problem
        if (tableNames.size() == 1) {
            // Copy the single table to a new temp table so we don't mess with the real, original table
            String copyTableName = copyTable(tableNames.get(0));
            return copyTableName;
        }
        
        // Pop the first two table names
        String t1 = tableNames.remove(0);
        TableSchema table1 = catalog.getTableFromName(t1);
        String t2 = tableNames.remove(0);
        TableSchema table2 = catalog.getTableFromName(t2);

        // Create a new table schema for the cartesian product
        TableSchema tempTable = new TableSchema("_temp_" + t1 + "_" + t2); // Name the temp table based on the two tables being combined

        // Combine attributes from both tables and rename them with table name as a prefix if not already prefixed
        ArrayList<AttrSchema> combinedAttributes = new ArrayList<>();
        for (AttrSchema attr : table1.getAttributes()) {
            String attrName = attr.getName();
            if (!attrName.contains(".")) { // Check if the attribute is already prefixed
                attrName = t1 + "." + attrName;
            }
            AttrSchema newAttr = new AttrSchema(attrName, attr.getType(), attr.getLength());
            newAttr.setConstraints(new ArrayList<>(attr.getConstraints())); // Set constraints separately
            combinedAttributes.add(newAttr);
        }
        for (AttrSchema attr : table2.getAttributes()) {
            String attrName = attr.getName();
            if (!attrName.contains(".")) { // Check if the attribute is already prefixed
                attrName = t2 + "." + attrName;
            }
            AttrSchema newAttr = new AttrSchema(attrName, attr.getType(), attr.getLength());
            newAttr.setConstraints(new ArrayList<>(attr.getConstraints())); // Set constraints separately
            combinedAttributes.add(newAttr);
        }
        tempTable.setAttributes(combinedAttributes);

        // Initialize metadata for the temporary table
        tempTable.setId(catalog.getNextId());
        catalog.setNextId(catalog.getNextId() + 1);
        tempTable.setNumPages(0);
        tempTable.setNumRecords(0);
        tempTable.setPageOrdering(new ArrayList<>());

        // Add the temporary table to the catalog
        catalog.addTempTable(tempTable);
        writeCatalog(dbLocation + File.separator + "catalog", catalog);

        
        // Create the .bin file for the temporary table
        // try (FileOutputStream fos = new FileOutputStream(dbLocation + File.separator + tempTable + ".bin")) {
        //     byte[] bytes = ByteBuffer.allocate(4).putInt(0).array(); // Write 0 pages initially
        //     fos.write(bytes);
        // } catch (Exception e) {
        //     e.printStackTrace();
        // }

        // Quadruple for loop going through the pages and records of both t1 and t2
        for (int pageId1 : table1.getPageOrdering()) {
            Page page1 = getPageFromBuffer(catalog.getIdFromName(t1), pageId1);
            for (int pageId2 : table2.getPageOrdering()) {
                Page page2 = getPageFromBuffer(catalog.getIdFromName(t2), pageId2);
                for (Record record1 : page1.getRecords()) {
                    for (Record record2 : page2.getRecords()) {
                        // Create a new record for the cartesian product
                        Record newRecord = new Record("temp");
                        ArrayList<Object> values = new ArrayList<>();
                        values.addAll(record1.getValues());
                        values.addAll(record2.getValues());
                        for( Object value : values) {
                            newRecord.addValue(value);
                        }
                        insertRecordToTable(newRecord, tempTable.getName(), false, false);
                    }
                }
            
            }
        }

        // Base case. If list is empty, return temp
        if (tableNames.size() == 0) {

            return tempTable.getName();
        }
        tableNames.add(0, tempTable.getName());
        return cartesianProduct(tableNames);
    }


    public String copyTable(String tableName) {
        TableSchema table = catalog.getTableFromName(tableName);
        if (table == null) {
            System.out.println("Error: Table \"" + tableName + "\" not found.");
            return "";
        }
        TableSchema tableCopy = new TableSchema(table.getName() + "_copy");
        ArrayList<AttrSchema> attributes = new ArrayList<>(); // Create a new list of attributes for the copied table
        for (AttrSchema attr : table.getAttributes()) {
            String fullAttributeName = table.getName() + "." + attr.getName();
            AttrSchema newAttr = new AttrSchema(fullAttributeName, attr.getType(), attr.getLength());
            
            // Copy the constraints from the original attribute to the new attribute
            // This ensures that the new attribute has the same constraints as the original one
            for (Constraint constraint : attr.getConstraints()) {
                newAttr.addConstraint(constraint); // Add each constraint to the new attribute
            }
            // Add the new attribute to the list of attributes for the copied table
            attributes.add(newAttr);
        }
        tableCopy.setAttributes(attributes);
        tableCopy.setId(catalog.getNextId());
        catalog.setNextId(catalog.getNextId() + 1);
        tableCopy.setNumPages(0);
        tableCopy.setNumRecords(0);
        tableCopy.setPageOrdering(new ArrayList<>());

        // Add the temporary table to the catalog
        catalog.addTempTable(tableCopy);
        writeCatalog(dbLocation + File.separator + "catalog", catalog);

        for (int pageId: table.getPageOrdering()) {
            Page page = getPageFromBuffer(catalog.getIdFromName(tableName), pageId);
            for (Record record : page.getRecords()) {
                Record newRecord = new Record("temp");
                ArrayList<Object> values = record.getValues(); // get the values from the original record
                for (Object value: values) {
                    newRecord.addValue(value); // add the values to the new record
                }
                insertRecordToTable(newRecord, tableCopy.getName(), false, false);
            }
        }



        return tableCopy.getName();
    }

    public void selectAll(String tableName) {

        TableSchema table = catalog.getTableFromName(tableName);
        File tableFile = new File(dbLocation + File.separator + tableName + ".bin");
        if (table == null || !tableFile.exists()) {
            System.out.println("Error: Table \"" + tableName + "\" not found.");
            return;
        }

        ArrayList<AttrSchema> attributes = table.getAttributes();
        StringBuilder attNames = new StringBuilder("|");
        for (AttrSchema attr : attributes) {
            String attrName = attr.getName();
            int lastDotIndex = attrName.lastIndexOf('.');
            int secondLastDotIndex = attrName.lastIndexOf('.', lastDotIndex - 1);
            String result = attrName.substring(secondLastDotIndex + 1);
            attNames.append(' ').append(result).append(" |");
        }
        String divider = ("-".repeat(Math.max(0, attNames.length())));
        System.out.println(divider + "\n" + attNames + "\n" + divider);
    
        try (FileInputStream fis = new FileInputStream(tableFile)) {
            byte[] intBytes = new byte[4];
            for (int i : table.getPageOrdering()) {
                // Read the number of records in the page
                int numpages = fis.read(intBytes);
                int numRecords = ByteBuffer.wrap(intBytes).getInt();
    
                Page page = getPageFromBuffer(catalog.getIdFromName(tableName), i);
    
                for (Record record : page.getRecords()) {
                    System.out.print("|");
                    for (int j = 0; j < attributes.size(); j++) {
                        Object value = record.getValues().get(j);
                        if (value == null) {
                            System.out.print(" null |");
                        } else if (attributes.get(j).getType() == DataType.CHAR || attributes.get(j).getType() == DataType.VARCHAR) {
                            System.out.print(" \"" + value.toString() + "\" |");
                        } else {
                            System.out.print(' ' + value.toString() + " |");
                        }
                    }
                    System.out.println();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void selectSpecificAttributes(String tableName, ArrayList<String> attributes) {
        TableSchema table = catalog.getTableFromName(tableName);
        File tableFile = new File(dbLocation + File.separator + tableName + ".bin");
        if (table == null || !tableFile.exists()) {
            System.out.println("Error: Table \"" + tableName + "\" not found.");
            return;
        }
    
        ArrayList<AttrSchema> allAttributes = table.getAttributes();
        ArrayList<Integer> attributeIndexes = new ArrayList<>();
        for (String attribute : attributes) {
            for (int i = 0; i < allAttributes.size(); i++) {
                String fullAttributeName = table.getName() + "." + allAttributes.get(i).getName();
                if (attribute.equals(fullAttributeName)) {
                    attributeIndexes.add(i);
                    break;
                }
            }
        }
    
        // Print header
        StringBuilder header = new StringBuilder("|");
        for (String attribute : attributes) {
            header.append(" ").append(attribute).append(" |");
        }
        String divider = "-".repeat(header.length());
        System.out.println(divider);
        System.out.println(header);
        System.out.println(divider);
    
        // Print records
        try (FileInputStream fis = new FileInputStream(tableFile)) {
            for (int pageId : table.getPageOrdering()) {
                Page page = getPageFromBuffer(catalog.getIdFromName(tableName), pageId);
                for (Record record : page.getRecords()) {
                    System.out.print("|");
                    for (int index : attributeIndexes) {
                        Object value = record.getValues().get(index);
                        if (value == null) {
                            System.out.print(" null |");
                        } else {
                            System.out.print(" " + value.toString() + " |");
                        }
                    }
                    System.out.println();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String orderBy(String tableName, String attributeName) {
        // this function will take in a table, and an attribute to order by, and return a table ordered by attribute
        TableSchema _temp = catalog.getTableFromName(tableName);
        if (_temp == null) {
            System.out.println("Error: Table \"" + tableName + "\" not found.");
            return "";
        }
        String tempTableName = copyTable(_temp.getName());
        TableSchema tempTable = catalog.getTableFromName(tempTableName);
        ArrayList<AttrSchema> attributes = tempTable.getAttributes();
        String newAttrName = tableName + "." + attributeName; // Create a new attribute name with the table name as a prefix
    
        boolean attributeFound = false;
    
        for (AttrSchema currAttr : attributes) {
            // Remove all constraints from all attributes
            currAttr.removeConstraint();
    
            // Check if the attribute name matches the one we want to order by
            if (currAttr.getName().equals(newAttrName)) {
                currAttr.addConstraint(Constraint.PRIMARYKEY); // Add the PRIMARYKEY constraint
                attributeFound = true;
            }
        }
    
        if (!attributeFound) {
            System.out.println("Error: Attribute \"" + attributeName + "\" not found in table \"" + tableName + "\".");
            return "";
        }
    
        tempTable.setAttributes(attributes);
        tempTable.setId(catalog.getNextTempId());
        catalog.setNextTempId(catalog.getNextTempId() - 1);
        tempTable.setNumPages(0);
        tempTable.setNumRecords(0);
        tempTable.setPageOrdering(new ArrayList<>());
    
        // Add the temporary table to the catalog
        catalog.addTempTable(tempTable);
        writeCatalog(dbLocation + File.separator + "catalog", catalog);
    
        for (int pageId : _temp.getPageOrdering()) {
            Page page = getPageFromBuffer(catalog.getIdFromName(tableName), pageId);
            for (Record record : page.getRecords()) {
                Record newRecord = new Record("temp");
                ArrayList<Object> values = record.getValues(); // get the values from the original record
                for (Object value : values) {
                    newRecord.addValue(value); // add the values to the new record
                }
                insertRecordToTable(newRecord, tempTable.getName(), false, false);
            }
        }
        return tempTable.getName();
    }
    
  
    public void parseInsert(String command) {
        command = command.substring("insert into ".length()).trim();
        String tableName = command.substring(0, command.indexOf(" ")).strip();
        TableSchema table = catalog.getTableFromName(tableName);
        System.out.println("Inserting records into table \"" + tableName + "\"...");
        if (table == null) {
            System.out.println("Table " + tableName + " not found.");
            return;
        }
        command = command.substring(tableName.length()).strip();
        if (!command.startsWith("values")) {
            System.out.println("Error: Expected keyword \"values\" but found \"" + command.substring(0, command.indexOf(" ")).strip() + '"');
        }
        command = command.substring("values".length()).strip();

        ArrayList<AttrSchema> attributes = table.getAttributes();
        int attrCount = attributes.size();
        //Start iterating through ( ) values
        while (!command.startsWith(";")) {
            if (command.isEmpty()) {
                System.out.println("Error: Unexpected end of command");
                return;
            }
            if (command.startsWith(",")) {
                command = command.substring(1).strip();
                continue;
            }
            else if (command.startsWith("(")) {
                command = command.substring(1).strip();
                Record newRecord = new Record("temp");
                int nullBitmap = 0;
                for (int i = 0; i < attrCount; i++) {
                    DataType targetDataType = attributes.get(i).getType();
                    Object valueToAdd;

                    String nextSeparator = (i < attrCount - 1 ? " " : ")");

                    if (!command.contains(nextSeparator)) {
                        System.out.println("Error: Expected more values");
                        return;
                    }

                    if (command.substring(0, command.indexOf(nextSeparator)).equals("null")) {
                        valueToAdd = null;
                        command = command.substring(("null").length()).strip();
                        nullBitmap = nullBitmap | 1;
                    } else {
                        switch (targetDataType) {
                            case INTEGER:
                                try {
                                    valueToAdd = Integer.parseInt(command.substring(0, command.indexOf(nextSeparator)).strip());
                                } catch (NumberFormatException e) {
                                    System.out.println("Error: Couldn't parse " + command.substring(0, command.indexOf(nextSeparator)).strip() + " as int");
                                    return;
                                }
                                command = command.substring(valueToAdd.toString().length());
                                break;
                            case DOUBLE:
                                int tempLength;
                                try {
                                    tempLength = command.indexOf(nextSeparator); //need to take the actual length of command given, weird double stuff
                                    valueToAdd = Double.parseDouble(command.substring(0, tempLength).strip());
                                } catch (NumberFormatException e) {
                                    System.out.println("Error: Couldn't parse " + command.substring(0, command.indexOf(nextSeparator)).strip() + " as double");
                                    return;
                                }
                                command = command.substring(tempLength);
                                break;
                            case BOOLEAN:
                                valueToAdd = command.substring(0, command.indexOf(nextSeparator)).strip();
                                if (!(valueToAdd.equals("true") || valueToAdd.equals("false"))) {
                                    System.out.println("Error: Couldn't parse " + command.substring(0, command.indexOf(nextSeparator)).strip() + " as boolean");
                                    return;
                                }
                                command = command.substring(valueToAdd.toString().length());
                                break;
                            case CHAR:
                                if (!command.startsWith("\"")) {
                                    System.out.println("Error: Expected \" to begin char value");
                                    return;
                                }
                                command = command.substring(1).strip();
                                valueToAdd = command.substring(0, command.indexOf('"')).strip();
                                if (valueToAdd.toString().length() > attributes.get(i).getLength()) {
                                    System.out.println("Error: Provided char exceeds max length");
                                    return;
                                }
                                command = command.substring(valueToAdd.toString().length() + 1);
                                break;
                            case VARCHAR:
                                if (!command.startsWith("\"")) {
                                    System.out.println("Error: Expected \" to begin varchar value");
                                    return;
                                }
                                command = command.substring(1).strip();
                                valueToAdd = command.substring(0, command.indexOf('"')).strip();
                                if (valueToAdd.toString().length() > attributes.get(i).getLength()) {
                                    System.out.println("Error: Provided varchar exceeds max length");
                                    return;
                                }
                                command = command.substring(valueToAdd.toString().length() + 1);
                                break;
                            default:
                                System.out.println("Error: Table had unexpected attribute type");
                                return;
                        }
                    }

                    //checks for integrity of new record
                    ArrayList<Constraint> constraints = attributes.get(i).getConstraints();
                    if (constraints.contains(Constraint.PRIMARYKEY) || constraints.contains(Constraint.NOTNULL)) {
                        if (valueToAdd == null) {
                            System.out.println("Error: Value in record cannot be null");
                            return;
                        }
                    }

                    nullBitmap = nullBitmap << 1;

                    newRecord.addValue(valueToAdd);
                    if (i < attrCount - 1) {
                        if (!command.startsWith(" ")) {
                            System.out.println("Error: Expected more space separated values");
                            return;
                        }
                        command = command.strip();
                    } else {
                        command = command.strip();
                        if (!command.startsWith(")")) {
                            System.out.println("Error: Expected end of record");
                            return;
                        }
                        command = command.substring(command.indexOf(")") + 1).strip();
                    }
                }
                newRecord.setNullBitmap(nullBitmap);
                //Record is built by this point, add it
                if (!insertRecordToTable(newRecord, tableName, true, false)) {
                    System.out.println("Aborting insert operation");
                    return;
                }
                //for debug, print record values
                /*System.out.println("Record to add:");
                for (Object o : newRecord.getValues()) {
                    if (o == null) {
                        System.out.println(" null");
                        continue;
                    }
                    System.out.println(" " + o.toString());
                }*/
            }
            else {
                System.out.println("Error: Expected ( or ,");
                return;
            }
        }
    }

    public String cleanWhereClause(String whereClause) {
        String[] tokens = whereClause.split(" ");
        StringBuilder newWhere = new StringBuilder();
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (token.contains(".")) {
                boolean isNumber = true;
                for (int j = 0; j < token.length(); j++) {
                    if (Character.isLetter(token.charAt(j))) {
                        isNumber = false;
                    }
                }
                if (isNumber) {
                    newWhere.append(token).append(" ");
                } else {
                    newWhere.append(token.substring(token.indexOf(".") + 1)).append(" ");
                }
            } else {
                newWhere.append(token).append(" ");
            }
        }
        return newWhere.toString();
    }

    public void parseDelete(String command) {
        command = command.substring("delete from ".length()).trim();
        String tableName = (command.contains(" ") ? command.substring(0, command.indexOf(' ')) : command.substring(0, command.indexOf(';')));
        TableSchema oldTable = catalog.getTableFromName(tableName);
        if (oldTable == null) {
            System.out.println("Error: Table " + tableName + " not found");
            return;
        }

        command = command.substring(tableName.length()).trim();
        if (!command.startsWith("where ")) {
            command = "where true" + command;
        }

        command = command.substring("where ".length()).trim();
        String whereClause = cleanWhereClause(command.substring(0, command.indexOf(';')));
        WhereBuilder wb = new WhereBuilder(oldTable.getAttributes());
        try {
            wb.parseStatement(whereClause);
        } catch (WhereSyntaxException | WhereSemanticException e) {
            System.out.println(e.getMessage());
        }

        /*DEBUG
        System.out.println("///DEBUG///\n");
        System.out.println("table name: " + tableName);
        System.out.println("where tree:\n" + wb);
        System.out.println("\n///");*/

        String tempTableName = "_temp_" + tableName;
        TableSchema newTable = new TableSchema(tempTableName);
        newTable.setAttributes(oldTable.getAttributes());
        catalog.addTable(newTable);

        //System.out.println("///DEBUG WHERE///\n");
        //System.out.println("del\t | record");
        for (int pageNum : oldTable.getPageOrdering()) {
            Page page = getPageFromBuffer(oldTable.getId(), pageNum);
            for (Record r : page.getRecords()) {
                //System.out.println((wb.parseRecord(r.getValues()) ? "yes" : "no") + "\t | " + r.getValues());
                if (!wb.parseRecord(r.getValues())) {
                    insertRecordToTable(r, tempTableName, true, false);
                }
            }
        }

        System.out.println("\nDeletion on table " + tableName + " complete.");
        //System.out.println("\n///");

        catalog.dropTableName(tableName);
        dropTableFromBuffer(oldTable.getId());
        renameTable(tempTableName, tableName);
        writeCatalog(dbLocation + File.separator + "catalog", catalog);
    }

    public void parseUpdate(String command) {
        command = command.substring("update ".length()).trim();
        String tableName = command.substring(0, command.indexOf(' '));
        TableSchema oldTable = catalog.getTableFromName(tableName);
        if (oldTable == null) {
            System.out.println("Error: Table " + tableName + " not found");
            return;
        }

        command = command.substring(tableName.length()).trim();
        if (!command.startsWith("set ")) {
            System.out.println("Error: Expected \"set\"");
            return;
        }

        command = command.substring("set ".length()).trim();
        String attrName = command.substring(0, command.indexOf(' '));
        AttrSchema setAttribute = null;
        for (int i = 0; i < oldTable.getAttributes().size(); i++) {
            if (oldTable.getAttributes().get(i).getName().equals(attrName)) {
                setAttribute = oldTable.getAttributes().get(i);
                break;
            }
        }
        if (setAttribute == null) {
            System.out.println("Error: Attribute " + attrName + " not found");
            return;
        }

        command = command.substring(command.indexOf(' ') + 1).trim();
        if (!command.startsWith("=")) {
            System.out.println("Error: Expected \"=\"");
            return;
        }

        command = command.substring(1).trim();
        String newValString;
        if (command.startsWith("\"")) {
            if (command.substring(1).indexOf('"') == -1) {
                System.out.println("Error: Expected \" to end string literal");
            }
            newValString = command.substring(0, command.indexOf('"') + 1);
            command = command.substring(newValString.length());
        } else {
            newValString = (command.contains(" ") ? command.substring(0, command.indexOf(' ')) : command.substring(0, command.indexOf(';')));
            command = command.substring(newValString.length());
        }

        Object setValue = null;
        switch (setAttribute.getType()) {
            case INTEGER:
                try {
                    setValue = Integer.parseInt(newValString);
                } catch (NumberFormatException e) {
                    System.out.println("Error: Couldn't parse \"" + newValString + "\" as int");
                    return;
                }
                break;
            case DOUBLE:
                try {
                    setValue = Double.parseDouble(newValString);
                } catch (NumberFormatException e) {
                    System.out.println("Error: Couldn't parse \"" + newValString + "\" as double");
                    return;
                }
                break;
            case BOOLEAN:
                if (newValString.equals("true")) {
                    setValue = true;
                } else if (newValString.equals("false")) {
                    setValue = false;
                } else {
                    System.out.println("Error: Couldn't parse \"" + newValString + "\" as boolean");
                    return;
                }
                break;
            case CHAR:
            case VARCHAR:
                if (!(newValString.charAt(0) == '"' && newValString.charAt(newValString.length() - 1) == '"')) {
                    System.out.println("Error: Couldn't parse " + newValString + " as string literal");
                    return;
                }
                newValString = newValString.substring(1, newValString.length() - 1);
                if (newValString.length() > setAttribute.getLength()) {
                    System.out.println("Error: Given string literal \"" + newValString + "\" is too long");
                    return;
                }
                setValue = newValString;
        }

        command = command.substring(command.indexOf(' ') + 1);
        if (!command.startsWith("where ")) {
            command = "where true" + command;
        }

        command = command.substring("where ".length()).trim();
        String whereClause = cleanWhereClause(command.substring(0, command.indexOf(';')));
        WhereBuilder wb = new WhereBuilder(oldTable.getAttributes());
        try {
            wb.parseStatement(whereClause);
        } catch (WhereSyntaxException | WhereSemanticException e) {
            System.out.println(e.getMessage());
        }

        /*DEBUG
        System.out.println("///DEBUG///\n");
        System.out.println("table name: " + tableName);
        System.out.println("attribute to set: " + setAttribute.getName());
        System.out.println("value to set: " + setValue);
        System.out.println("where tree:\n" + wb);
        System.out.println("\n///");*/

        String tempTableName = "_temp_" + tableName;
        TableSchema newTable = new TableSchema(tempTableName);
        newTable.setAttributes(oldTable.getAttributes());
        catalog.addTable(newTable);

        int attrIndex;
        for (attrIndex = 0; attrIndex < oldTable.getAttributes().size(); attrIndex++) {
            if (oldTable.getAttributes().get(attrIndex).getName().equals(attrName)) {
                break;
            }
        }
        //System.out.println("attribute index: " + attrIndex);

        //System.out.println("///DEBUG UPDATE///\n");
        //System.out.println("del\t | record");
        boolean updateFailed = false;
        for (int pageNum : oldTable.getPageOrdering()) {
            Page page = getPageFromBuffer(oldTable.getId(), pageNum);
            for (int i = 0; i < page.getRecords().size(); i++) {
                Record r = page.getRecords().get(i);
                //System.out.println((wb.parseRecord(r.getValues()) ? "yes" : "no") + "\t | " + r.getValues());
                if (!wb.parseRecord(r.getValues()) || updateFailed) {
                    insertRecordToTable(r, tempTableName, true, false);
                } else {
                    Record newRecord = new Record("blah");
                    newRecord.setNullBitmap(r.getNullBitmap());
                    for (int j = 0; j < r.getValues().size(); j++) {
                        if (j != attrIndex) {
                            newRecord.addValue(r.getValues().get(j));
                        } else {
                            newRecord.addValue(setValue);
                        }
                    }
                    boolean success = insertRecordToTable(newRecord, tempTableName, true, false);
                    if (!success) {
                        updateFailed = true;
                        i--;
                        System.out.println("Aborting update process, remaining records will be unchanged.");
                    }
                }
            }
        }

        System.out.println("\nUpdate on table " + tableName + " complete.");
        //System.out.println("\n///");

        catalog.dropTableName(tableName);
        dropTableFromBuffer(oldTable.getId());
        renameTable(tempTableName, tableName);
        writeCatalog(dbLocation + File.separator + "catalog", catalog);
    }

    public void parseAlterTable(String command) {
        command = command.substring("alter table ".length()).trim();
        String tableName = command.substring(0, command.indexOf(" ")).trim();
        System.out.println("Altering table \"" + tableName + "\"...");
        TableSchema oldTable = catalog.getTableFromName(tableName);
        if (oldTable == null) {
            System.out.println("Error: Table " + tableName + " not found");
            return;
        }
        ArrayList<AttrSchema> oldAttributes = oldTable.getAttributes();
        //System.out.println("Debug: " + tableName);
        command = command.substring(command.indexOf(" ")).trim();
        //System.out.println("Debug: " + command);

        if(command.startsWith("drop "))
        {
            //System.out.println("Debug: INSIDE DROP IF");
            command = command.substring("drop ".length()).trim();
            //System.out.println("Debug: " + command);
            String attrName = command.substring(0, command.indexOf(";")).trim();
            //System.out.println("Debug: " + attrName);

            int dropIndex;
            for (dropIndex = 0; dropIndex < oldAttributes.size(); dropIndex++) {
                if (oldAttributes.get(dropIndex).getName().equals(attrName)) {
                    break;
                }
            }
            if (dropIndex == oldAttributes.size()) {
                System.out.println("Error: Attribute " + attrName + " not found");
                return;
            }
            if (oldAttributes.get(dropIndex).getConstraints().contains(Constraint.PRIMARYKEY)) {
                System.out.println("Error: Can't drop primary attribute");
                return;
            }

            ArrayList<Record> newRecords = new ArrayList<>(oldTable.getNumRecords());
            for (int i : oldTable.getPageOrdering()) {
                Page page = getPageFromBuffer(oldTable.getId(), i);
                for (Record r : page.getRecords()) {
                    int nullBitmap = 0;
                    Record newRecord = new Record("blah");
                    for (int j = 0; j < oldAttributes.size(); j++) {
                        if (j != dropIndex) {
                            newRecord.addValue(r.getValues().get(j));
                            if (r.getValues().get(j) == null) {
                                nullBitmap = nullBitmap | 1;
                            }
                            nullBitmap = nullBitmap << 1;
                        }
                    }
                    newRecord.setNullBitmap(nullBitmap);
                    newRecords.add(newRecord);
                }
            }

            TableSchema newTable = new TableSchema(oldTable.getName());
            newTable.setId(oldTable.getId());
            ArrayList<AttrSchema> newAttributes = oldTable.getAttributes();
            newAttributes.remove(dropIndex);
            newTable.setAttributes(newAttributes);
            catalog.dropTableName(tableName);
            catalog.addExistingTable(newTable);
            writeCatalog(dbLocation + File.separator + "catalog", catalog);
            File oldTableFile = new File(dbLocation + File.separator + tableName + ".bin");
            oldTableFile.delete();

            for (Record r : newRecords) {
                insertRecordToTable(r, tableName, true, false);
            }

        }
        else if(command.startsWith("add "))
        {
            //System.out.println("Debug: INSIDE ADD IF");
            command = command.substring("add ".length()).trim();
            //System.out.println("Debug: " + command);

            String[] parts = command.split("\\s+");
            if (parts.length < 2) {
                System.out.println("Error: Invalid add column command.");
                return;
            }

            String attr_name = parts[0];
            String attr_type = parts[1].replace(";", "");
            //System.out.println("Debug: " + attr_name);
            //System.out.println("Debug: " + attr_type);
            for (AttrSchema attr : oldAttributes) {
                if (attr.getName().equals(attr_name)) {
                    System.out.println("Error: Attribute with name " + attr.getName() + " already exists");
                    return;
                }
            }
            
            DataType type;
            int length = 0;
            if (attr_type.toUpperCase().equals("INTEGER")) {
                type = DataType.INTEGER;
            }
            else if (attr_type.toUpperCase().equals("DOUBLE")) {
                type = DataType.DOUBLE;
            }
            else if (attr_type.toUpperCase().equals("BOOLEAN")) {
                type = DataType.BOOLEAN;
            }
            else if (attr_type.toUpperCase().startsWith("CHAR(")) {
                type = DataType.CHAR;
                String attr_length = attr_type.substring(("CHAR(").length(), attr_type.indexOf(")"));
                length = Integer.parseInt(attr_length);
            }
            else if (attr_type.toUpperCase().startsWith("VARCHAR(")) {
                type = DataType.VARCHAR;
                String attr_length = attr_type.substring(("VARCHAR(").length(), attr_type.indexOf(")"));
                length = Integer.parseInt(attr_length);
            }
            else {
                System.out.println("Error: Invalid data type");
                return;
            }

            Object defaultValue = null;
            if (parts.length > 3 && parts[2].equalsIgnoreCase("default")) {
                //System.out.println("Debug: INSIDE IF" + parts[3]);
                String defaultValueString = parts[3].replace(";", "");
                switch (type) {
                    case INTEGER:
                        try {
                            defaultValue = Integer.parseInt(defaultValueString);
                        } catch (NumberFormatException e) {
                            System.out.println("Error: Couldn't parse " + defaultValueString + " as int");
                            return;
                        }
                        break;
                    case DOUBLE:
                        try {
                            defaultValue = Double.parseDouble(defaultValueString);
                        } catch (NumberFormatException e) {
                            System.out.println("Error: Couldn't parse " + defaultValueString + " as double");
                            return;
                        }
                        break;
                    case BOOLEAN:
                        defaultValue = defaultValueString;
                        if (!(defaultValue.equals("true") || defaultValue.equals("false"))) {
                            System.out.println("Error: Couldn't parse " + defaultValueString + " as boolean");
                            return;
                        }
                        break;
                    case CHAR:
                    case VARCHAR:
                        if (!defaultValueString.startsWith("\"")) {
                            System.out.println("Error: Expected \" to begin string value");
                            return;
                        }
                        defaultValueString = defaultValueString.substring(1).strip();
                        defaultValue = defaultValueString.substring(0, defaultValueString.indexOf('"')).strip();
                        if (defaultValue.toString().length() > length) {
                            System.out.println("Error: Provided string exceeds max length");
                            return;
                        }
                        break;
                }
                //System.out.println("default value: " + defaultValue);
            }

            ArrayList<Record> newRecords = new ArrayList<>(oldTable.getNumRecords());
            for (int i : oldTable.getPageOrdering()) {
                Page page = getPageFromBuffer(oldTable.getId(), i);
                for (Record r : page.getRecords()) {
                    int nullBitmap = r.getNullBitmap();
                    if (defaultValue == null) {
                        nullBitmap = nullBitmap | 1;
                        r.addValue(null);
                    } else {
                        r.addValue(defaultValue);
                    }
                    nullBitmap = nullBitmap << 1;
                    r.setNullBitmap(nullBitmap);
                    newRecords.add(r);
                }
            }

            TableSchema newTable = new TableSchema(oldTable.getName());
            newTable.setId(oldTable.getId());
            ArrayList<AttrSchema> newAttributes = oldTable.getAttributes();
            AttrSchema newAttribute = new AttrSchema(attr_name, type, length);
            newAttributes.add(newAttribute);
            newTable.setAttributes(newAttributes);
            catalog.dropTableName(tableName);
            catalog.addExistingTable(newTable);
            writeCatalog(dbLocation + File.separator + "catalog", catalog);
            File oldTableFile = new File(dbLocation + File.separator + tableName + ".bin");
            oldTableFile.delete();

            for (Record r : newRecords) {
                insertRecordToTable(r, tableName, true, false);
            }

        }
        else
        {
            System.out.println("Error: Unsupported alter table command.");
            return;
        }
    

        // command = command.substring(command.indexOf(" ")).trim();
        // System.out.println("Debug: " + command);

        // if (command.startsWith("drop ")) {
        //     command = command.substring("drop ".length()).trim();
        //     String columnName = command.substring(0, command.indexOf(";")).trim();

        //     for (TableSchema table : catalog.getTables()) {
        //         if (table.getName().equals(tableName)) {
        //             //table.dropColumn(columnName);
        //             System.out.println("Column " + columnName + " dropped from table " + tableName);
        //             return;
        //         }
        //     }
        //     System.out.println("Error: Table " + tableName + " not found.");
        // } else {
        //     System.out.println("Error: Unsupported alter table command.");
        // }
    }

    public void removeTemporaryTables() {
        ArrayList<Integer> tempTableIds = new ArrayList<>();
        // Iterate through the catalog to find all temporary tables
        for (TableSchema table : catalog.getTables()) {
            if (table.getId() < 0) {
                tempTableIds.add(table.getId()); // Store the IDs of temporary tables (negative IDs)
            }
        }
        // Remove each temporary table from the catalog and the page buffer
        for (Integer tempTableId : tempTableIds) {
            TableSchema currentTempTable = catalog.getTableFromId(tempTableId);
            // Drop the temporary table from the catalog
            catalog.dropTableId(tempTableId);
            // Update the catalog file to remove the temporary table's schema
            writeCatalog(dbLocation + File.separator + "catalog", catalog);
            File tempTableFile = new File(dbLocation + File.separator + currentTempTable.getName() + ".bin");
            if (tempTableFile.exists()) {
                // Delete the temporary table's .bin file
                tempTableFile.delete();
            }
            // Remove the temporary table's pages from the page buffer
            dropTableFromBuffer(tempTableId);
        }
        return;
    }

    public void dropAttributeForSelect(String tableName, ArrayList<String> keptAttributes) {
        TableSchema oldTable = catalog.getTableFromName(tableName);
        if (oldTable == null) {
            System.out.println("Error: Table \"" + tableName + "\" not found.");
            return;
        }
        ArrayList<AttrSchema> oldAttributes = oldTable.getAttributes();
        while (oldAttributes.size() != keptAttributes.size()) {

            int dropIndex;
            for (dropIndex = 0; dropIndex < oldAttributes.size(); dropIndex++) {
                boolean keepThisAttribute = false;
                for (String attributeName : keptAttributes) {
                    if (oldAttributes.get(dropIndex).getName().equals(attributeName)) {
                        // we want to keep this attribute, so we will not drop it
                        // continue to the next attribute in the loop
                        keepThisAttribute = true;
                    }
                }
                if (!keepThisAttribute) {
                    // We found an attribute that is not in the keptAttributes list, so we will drop it
                    break; // break out of the loop to drop this attribute
                }
            }

            // TODO figure out if I need to keep this or not
            if (dropIndex == oldAttributes.size()) {
                System.out.println("Error: Attribute " + "AAAHHHAHAH" + " not found");
                return;
            }

            // WE DO NOT CARE WHETHER WE ARE DROPPING A PRIMARY KEY OF A TEMPORARY TABLE IN SELECT
            // if (oldAttributes.get(dropIndex).getConstraints().contains(Constraint.PRIMARYKEY)) {
            //     System.out.println("Error: Can't drop primary attribute");
            //     return;
            // }

            ArrayList<Record> newRecords = new ArrayList<>(oldTable.getNumRecords());
            for (int i : oldTable.getPageOrdering()) {
                Page page = getPageFromBuffer(oldTable.getId(), i);
                for (Record r : page.getRecords()) {
                    int nullBitmap = 0;
                    Record newRecord = new Record("blah");
                    for (int j = 0; j < oldAttributes.size(); j++) {
                        if (j != dropIndex) {
                            newRecord.addValue(r.getValues().get(j));
                            if (r.getValues().get(j) == null) {
                                nullBitmap = nullBitmap | 1;
                            }
                            nullBitmap = nullBitmap << 1;
                        }
                    }
                    newRecord.setNullBitmap(nullBitmap);
                    newRecords.add(newRecord);
                }
            }

            TableSchema newTable = new TableSchema(oldTable.getName());
            newTable.setId(oldTable.getId());
            ArrayList<AttrSchema> newAttributes = oldTable.getAttributes();
            newAttributes.remove(dropIndex);
            newTable.setAttributes(newAttributes);
            catalog.dropTableName(tableName);
            catalog.addExistingTable(newTable);
            writeCatalog(dbLocation + File.separator + "catalog", catalog);
            File oldTableFile = new File(dbLocation + File.separator + tableName + ".bin");
            oldTableFile.delete();

            for (Record r : newRecords) {
                insertRecordToTable(r, tableName, false, true);
            }

            // Now we have to get the updated table and its attributes again to know if we
            // should continute the while loop.
            oldTable = catalog.getTableFromName(tableName);
            if (oldTable == null) {
                System.out.println("Error: Table \"" + tableName + "\" not found after dropping attribute.");
                return;
            }
            oldAttributes = oldTable.getAttributes(); // Get the updated attributes after dropping one
        }
    }

    public String orderBYClause(String attr, ArrayList<String> tableNamesList, String givenTableName) {
        // Check if the attribute exists in the resulting table after the cartesian product
        boolean attributeExists = false;
        String newAttrName = "";
        if (attr.contains(".")) {
            String[] attrParts = attr.split("\\.");
            String tableName = attrParts[0].trim();
            String attributeName = attrParts[1].trim();
            boolean tableExists = false;
            for (String tableNameInList : tableNamesList) {
                if (tableNameInList.equals(tableName)) {
                    tableExists = true;
                }
            }
            if (!tableExists) {
                System.out.println("ERROR -- Table " + tableName + " is not included in this query.");
                return "";
            }
            
            newAttrName = tableName + "." + attributeName; // Assuming the attribute belongs to the first table in the list
        }
        else {
            if (tableNamesList.size() == 1) {
                newAttrName = tableNamesList.get(0) + "." + attr; // Assuming the attribute belongs to the first table in the list
            }
            else {
                System.out.println("ERROR -- Attribute name is ambiguous. Please specify the table name.");
                return "";
            }
            
        }
        if (tableNamesList.size() == 1) {
            TableSchema resultingTable = catalog.getTableFromName(tableNamesList.get(0)); // Get the resulting table from the cartesian product
            for (AttrSchema attrSchema : resultingTable.getAttributes()) {
                // Check if the attribute exists in the resulting table's attributes

                String fullAttrName = tableNamesList.get(0) + "." + attrSchema.getName();
                if (fullAttrName.equals(newAttrName)) {
                    attributeExists = true;
                    break;
                }
            }
            if (!attributeExists) {
                System.out.println("ERROR -- Attribute " + attr + " does not exist in the resulting table after the cartesian product.");
                return "";
            }

        }
        else {
            for (String tableName : tableNamesList) {
                TableSchema resultingTable = catalog.getTableFromName(tableName); // Get the resulting table from the cartesian product
                for (AttrSchema attrSchema : resultingTable.getAttributes()) {
                    // Check if the attribute exists in the resulting table's attributes
                    String fullAttrName = tableName + "." + attrSchema.getName();
                    if (fullAttrName.equals(newAttrName)) {
                        attributeExists = true;
                        break;
                    }
                }
            }
            if (!attributeExists) {
                System.out.println("ERROR -- Attribute " + attr + " does not exist in the resulting table after the cartesian product.");
                return "";
            }
        }
        String orderedTableName = orderBy(givenTableName, newAttrName);
        return orderedTableName;
    }
}
