package db61b;

import java.util.ArrayList;
import static org.junit.Assert.*;
import java.io.IOException;
import org.junit.Test;

public class JUnitTest {
    /**
     * Test Row
     */
    @Test
    public void rowTest() throws IOException {
        Table table = new Table("students", new String[] {
            "SID", "Lastname", "Firstname", "SemEnter" });

        Column sid = new Literal("SID");
        Column last = new Literal("Lastname");
        Column first = new Literal("Firstname");
        Column sem = new Literal("SemEnter");

        Row exp = new Row(new String[] { "SID", "Lastname",
            "Firstname", "SemEnter" });
        ArrayList<Column> col = new ArrayList<Column>();
        col.add(sid);
        col.add(last);
        col.add(first);
        col.add(sem);
        Row row = new Row(col);
        assertEquals(exp, row);
        assertEquals(row.size(), 4);

    }

    /**
     * Test Table
     */
    @Test(expected = DBException.class)
    public void tableTest() throws IOException {
        Table table = new Table("students", new String[] {
            "SID", "Lastname", "Firstname", "SemEnter" });
        Row row = new Row(new String[] { "101", "Knowles", "Jason", "F" });
        table.add(row);
        table.print();
        Table tableBlank = Table.readTable("blank");
        Table expected = new Table("blank", new String[]
            { "First", "Second", "Third" });
        assertEquals(expected.columnIndex("Second"),
                tableBlank.columnIndex("Second"));
        assertEquals(expected.size(), tableBlank.size());

        Table enrolled = Table.readTable("enrolled");
        assertEquals(19, enrolled.getRows().size());
        assertEquals(3, enrolled.numColumns());
        assertEquals("enrolled", enrolled.name());
        Table table3 = new Table("students", new String[]
            { "SID", "Lastname", "Firstname", "SID" });

    }

    @Test
    public void tableIteratorTest() throws IOException {
        Table enr = Table.readTable("enrolled");
        TableIterator iter = new TableIterator(enr);
        assertEquals(enr.getRows().get(1), iter.next());
        assertEquals(enr.getRows().get(2), iter.next());
        iter.reset();
        assertEquals(enr.getRows().get(1), iter.next());
        assertEquals("21105", iter.value(1));
        iter.reset();
        for (int i = 0; i < iter.table().getRows().size() - 1; i++) {
            iter.next();
        }
        assertEquals(true, iter.hasRow());

    }

    @Test
    public void conditionTest() throws IOException {
        Column col1 = new Literal("101");
        Column col2 = new Literal("101");
        Condition cond1 = new Condition(col1, "<", "103");
        Condition cond2 = new Condition(col1, ">", col2);
        Condition cond3 = new Condition(col1, ">=", col2);

        Column strCol1 = new Literal("Jim");
        Column strCol2 = new Literal("Dave");
        Condition cond4 = new Condition(strCol1, ">", strCol2);

        assertEquals(true, cond1.test());
        assertEquals(false, cond2.test());
        assertEquals(true, cond3.test());
        assertEquals(true, cond3.test());
        assertEquals(true, cond4.test());
    }

    public static void main(String[] args) {
        System.exit(ucb.junit.textui.runClasses(JUnitTest.class));
    }
}
