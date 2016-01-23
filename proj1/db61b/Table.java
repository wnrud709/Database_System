package db61b;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static db61b.Utils.*;

/**
 * A single table in a database.
 * @author JuKyung Choi
 */
class Table implements Iterable<Row> {
    /**
     * A new Table named NAME whose columns are give by COLUMNTITLES, which must
     * be distinct (else exception thrown).
     */
    Table(String name, String[] columnTitles) {
        _name = name;
        _titles = columnTitles;
        _arrRow = new ArrayList<Row>();
        for (int i = 0; i < _titles.length; i++) {
            if (i != _titles.length - 1) {
                for (int j = i + 1; j < _titles.length; j++) {
                    if (_titles[i].equals(_titles[j])) {
                        throw error("Cannot have duplicate column names");
                    }
                }
            }
        }
    }

    /** A new Table named NAME whose column names are give by COLUMNTITLES. */
    Table(String name, List<String> columnTitles) {
        this(name, columnTitles.toArray(new String[columnTitles.size()]));
    }

    /** Return the number of columns in this table. */
    int numColumns() {
        /** IW */
        return _titles.length;
    }

    /** Returns my name. */
    String name() {
        return _name;
    }

    /** Returns a TableIterator over my rows in an unspecified order. */
    TableIterator tableIterator() {
        return new TableIterator(this);
    }

    /** Returns an iterator that returns my rows in an unspecified order. */
    @Override
    public Iterator<Row> iterator() {
        return _arrRow.iterator();
    }

    /** Return the title of the Kth column. Requires 0 <= K < columns(). */
    String title(int k) {
        return _titles[k];
    }

    /**
     * Return the number of the column whose title is TITLE, or -1 if there
     * isn't one.
     */
    int columnIndex(String title) {
        int count = 0;
        for (int i = 0; i < _titles.length; i++) {
            count += 1;
            if (_titles[i].equals(title)) {
                return count - 1;
            }
        }
        return -1;
    }

    /** Return the number of Rows in this table. */
    int size() {
        return _arrRow.size();
    }

    /**
     * Add ROW to THIS if no equal row already exists. Return true if anything
     * was added, false otherwise.
     */
    boolean add(Row row) {
        for (int i = 0; i < _arrRow.size(); i++) {
            if (row.equals(_arrRow.get(i))) {
                return false;
            }
        }
        _arrRow.add(row);
        return true;
    }

    /**
     * Read the contents of the file NAME.db, and return as a Table. Format
     * errors in the .db file cause a DBException.
     */
    static Table readTable(String name) {
        BufferedReader input;
        Table table;
        input = null;
        table = null;
        String thisLine = null;
        try {
            input = new BufferedReader(new FileReader(name + ".db"));
            String header = input.readLine();
            if (header == null) {
                throw error("missing header in DB file");
            }
            String[] columnNames = header.split(",");
            table = new Table(name, columnNames);
            while ((thisLine = input.readLine()) != null) {
                String[] row = thisLine.split(",");
                if (row.length == columnNames.length) {
                    table.add(new Row(row));
                } else {
                    throw error("Number of columns per row do not match.");
                }
            }
        } catch (FileNotFoundException e) {
            throw error("could not find %s.db", name);
        } catch (IOException e) {
            throw error("problem reading from %s.db", name);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    /* Ignore IOException */
                }
            }
        }
        return table;
    }

    /**
     * Write the contents of TABLE into the file NAME.db. Any I/O errors cause a
     * DBException.
     */
    void writeTable(String name) {
        PrintStream output;
        output = null;
        try {
            output = new PrintStream(name + ".db");
            for (int i = 0; i < _titles.length; i++) {
                if (i != _titles.length - 1) {
                    output.print(_titles[i] + ",");
                } else {
                    output.print(_titles[i]);
                }
            }
            output.println("");
            for (int i = 0; i < _arrRow.size(); i++) {
                for (int j = 0; j < _titles.length; j++) {
                    if (j != _titles.length - 1) {
                        output.print(_arrRow.get(i).get(j) + ",");
                    } else {
                        output.print(_arrRow.get(i).get(j));
                    }
                }
                output.println("");
            }
        } catch (IOException e) {
            throw error("trouble writing to %s.db", name);
        } finally {
            if (output != null) {
                output.close();
            }
        }
    }

    /**
     * Print my contents on the standard output, separated by spaces and
     * indented by two spaces.
     */
    void print() {
        for (int i = 0; i < _arrRow.size(); i++) {
            System.out.print("  ");
            for (int j = 0; j < _titles.length; j++) {
                System.out.print(_arrRow.get(i).get(j));
                if (j < _titles.length - 1) {
                    System.out.print(" ");
                }
            }
            System.out.println("");
        }
    }

    /**
     * Gets the arraylist of rows of the table.
     * @return arraylist of rows
     */
    ArrayList<Row> getRows() {
        return _arrRow;
    }

    /** My name. */
    private final String _name;
    /** My column titles. */
    private String[] _titles;
    /** List of rows. */
    private ArrayList<Row> _arrRow;
}
