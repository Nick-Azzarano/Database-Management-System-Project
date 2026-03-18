package Classes;

import java.util.ArrayList;

public class Record {
    private String name;
    private ArrayList<Object> values;
    private int nullBitmap;

    public Record(String name) {
        this.name = name;
        this.values = new ArrayList<>();
        this.nullBitmap = 0;
    }

    public void addValue(Object value) {
        values.add(value);
        if (value == null) {
            nullBitmap = nullBitmap | 1;
        }
        nullBitmap = nullBitmap << 1;
    }

    public String getName() {
        return name;
    }

    public ArrayList<Object> getValues() {
        return values;
    }

    public int getNullBitmap() {
        return nullBitmap;
    }

    public void setNullBitmap(int nullBitmap) {
        this.nullBitmap = nullBitmap;
    }
}