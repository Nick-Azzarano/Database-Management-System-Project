package WhereTree;

import Classes.AttrSchema;
import Classes.DataType;

import java.util.ArrayList;

public class WhereBuilderTester {

    public static void main(String[] args) {
        ArrayList<AttrSchema> attributes1 = new ArrayList<>();
        attributes1.add(new AttrSchema("foo", DataType.INTEGER, 0));
        attributes1.add(new AttrSchema("baz", DataType.DOUBLE, 0));
        attributes1.add(new AttrSchema("str", DataType.CHAR, 8));
        WhereBuilder wb = new WhereBuilder(attributes1);



        String testStatement1 = "foo = 1 and baz > 5.0 or   str =  \" hello\" and foo = 2";
        String testStatement2 = "foo = 1 and baz > 1.0 or foo = 2 and str = \"hello\"";
        String testStatement3 = "str = \"fail"; //fails because no closing quote
        String testStatement4 = "1 = foo and baz > 1.0"; //fails because literal value on left side
        String testStatement5 = "foo = 1 or baz = 7.8";

        try {
            wb.parseStatement(testStatement5);
            System.out.println(wb);

            ArrayList<Object> testValues1 = new ArrayList<>();
            testValues1.add(1);
            testValues1.add(5.1);
            testValues1.add(" no");
            ArrayList<Object> testValues2 = new ArrayList<>();
            testValues2.add(2);
            testValues2.add(4.1);
            testValues2.add(" hello");
            ArrayList<Object> testValues3 = new ArrayList<>();
            testValues3.add(5);
            testValues3.add(7.8);
            testValues3.add("test");

            System.out.println("Testing record: " + testValues1);
            System.out.println(wb.parseRecord(testValues1));
            System.out.println("Testing record: " + testValues2);
            System.out.println(wb.parseRecord(testValues2));
            System.out.println("Testing record: " + testValues3);
            System.out.println(wb.parseRecord(testValues3));
        } catch (WhereSyntaxException e) {
            System.out.println(e.getMessage());
        } catch (WhereSemanticException e) {
            System.out.println(e.getMessage());
        }
    }

}
