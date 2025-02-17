package simpledb.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    /**
     * 
     * @author Elliot
     * @brief Table is a Class that holds the table name, primary key field, and the
     *        table content (DbFile).
     *        The table id can be retrieve using getId of the DbFile interface.
     */
    public class Table {

        private String name;
        private String primaryKeyField;
        private DbFile tableContent;

        public Table(String name, String primaryKeyField, DbFile dbFile) {
            this.name = name;
            this.primaryKeyField = primaryKeyField;
            this.tableContent = dbFile;
        }

        public Table(String primaryKeyField, DbFile dbFile) {
            this.name = "";
            this.primaryKeyField = primaryKeyField;
            this.tableContent = dbFile;
        }

        public DbFile getContent() {
            return this.tableContent;
        }

        public String getPrimaryKeyField() {
            return this.primaryKeyField;
        }

        public String getName() {
            return this.name;
        }
    }

    private HashMap<Integer, Table> tablesMap;
    private HashMap<String, Integer> idNameMap;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */

    /**
     * 
     * @author Elliot
     * @brief Catalogue is a class with one attribute:
     *        HashMap<Integer, Table> tablesMap : key refers to id of Table instance
     *        (dbFile.getId), value is Table instance.
     */
    public Catalog() {
        // some code goes here
        this.tablesMap = new HashMap<>();
        this.idNameMap = new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * If there exists a table with the same name or ID, replace that old table with
     * this one.
     * 
     * @param file      the contents of the table to add; file.getId() is the
     *                  identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and
     *                  getFile.
     * @param name      the name of the table -- may be an empty string. May not be
     *                  null.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        if (name.isEmpty()) {
            tablesMap.put(file.getId(), new Table(pkeyField, file));
        } else {
            tablesMap.put(file.getId(), new Table(name, pkeyField, file));
        }
        idNameMap.put(name, file.getId());

    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * 
     * @param file the contents of the table to add; file.getId() is the identfier
     *             of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * 
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if (idNameMap.containsKey(name)) {
            return idNameMap.get(name);
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * 
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        return getDatabaseFile(tableid).getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * 
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        return getTable(tableid).getContent();
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        return getTable(tableid).getPrimaryKeyField();
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return idNameMap.values().iterator();
    }

    public String getTableName(int id) throws NoSuchElementException {
        // some code goes here
        return getTable(id).getName();
    }

    /**
     * Additional method
     * @author Elliot
     * @brief get Table instance by (int) table id
     * @param id (int): table id
     */
    public Table getTable(int id) throws NoSuchElementException {
        if (tablesMap.containsKey(id)) {
            return tablesMap.get(id);
        }
        throw new NoSuchElementException();
    }

    /**
     * Additional method
     * 
     * @author Elliot
     * @brief get Table instance by (String) table name
     * @param name (int): table name
     */
    public Table getTable(String name) throws NoSuchElementException {
        if (idNameMap.containsKey(name)) {
            return getTable(idNameMap.get(name));
        }
        throw new NoSuchElementException();
    }

    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        tablesMap.clear();
        idNameMap.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the
     * database.
     * 
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                // assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                // System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
