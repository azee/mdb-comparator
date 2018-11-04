package ru.greatbit.mdb.comparator;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class Comparer {

    private final static int ERRORS_LIMIT_PER_TABLE = 10;

    public static List<Error> compare(String path1, String path2, int tolerance) throws SQLException, ClassNotFoundException, IOException {
        List<Error> errors = new LinkedList<>();

        Set<String> tables1 = new TreeSet<>();
        Database db1 = new DatabaseBuilder(new File(path1)).open();
        tables1.addAll(db1.getTableNames());

        Set<String> tables2 = new TreeSet<>();
        Database db2 = new DatabaseBuilder(new File(path2)).open();
        tables2.addAll(db2.getTableNames());

        if (validateTablesListsEqual(tables1, tables2)){
            Error error = new Error(format("Found difference in columns lists. Got %s for the 1st file and %s for tne 2-d", tables1, tables2),
                    path1, path2);
            errors.add(error);
        }

        for (String table : tables1){
            List<Error> gotErrors = tablesEqual(db1, db2, table, tolerance).stream().
                    filter(Objects::nonNull).
                    collect(Collectors.toList());
            errors.addAll(gotErrors);
        }

        return errors;
    }

    private static List<Error> tablesEqual(Database db1, Database db2, String tableName, int tolerance) throws IOException {
        Table table1 = db1.getTable(tableName);
        Table table2 = db2.getTable(tableName);



        List<Error> errors = new LinkedList<>();

        if (table1 == null){
            return Collections.singletonList(
                    new Error(format("Table %s not found in file %s", tableName, db1.getFile().getPath()))
            );
        }

        if (table2 == null){
            return Collections.singletonList(
                new Error(format("Table %s not found in file %s", tableName, db2.getFile().getPath()))
            );
        }

        if (table1.getRowCount() != table2.getRowCount()){
            return Collections.singletonList(
                    new Error(format("Found different number of rows: %s and %s, for table %s",
                    table1.getRowCount(), table2.getRowCount(), tableName))
            );
        }

        String file1Path = db1.getFile().getAbsolutePath();
        String file2Path = db2.getFile().getAbsolutePath();
        errors.addAll(copareRows(table1, table2, file1Path, file2Path, tolerance));

        return errors;
    }

    private static Collection<? extends Error> copareRows(Table table1, Table table2,
                                                          String file1Path, String file2Path,
                                                          int tolerance) throws IOException {

        List<Error> errors = new LinkedList<>();
        String tableName = table1.getName();

        List<Map<String, Object>> rows1 = new LinkedList<>();
        List<Map<String, Object>> rows2 = new LinkedList<>();


        for (int i = 0; i < table1.getRowCount(); i++){
            rows1.add(table1.getNextRow());
            rows2.add(table2.getNextRow());
        }

        if (rows1.size() == 0){
            return errors;
        }

        for (String key : rows1.get(0).keySet()){
            if (!rows2.get(0).keySet().contains(key)){
                return Collections.singletonList(
                        new Error(format("Column %s exists in the %s document but missing in %s for table %s",
                        key, file1Path, file2Path, tableName))
                );
            }
        }

        for (String key : rows2.get(0).keySet()){
            if (!rows1.get(0).keySet().contains(key)){
                return Collections.singletonList(
                        new Error(format("Column %s exists in the %s document but missing in %s for table %s",
                        key, file1Path, file2Path, tableName))
                );
            }
        }

        for (int i = 0; i < rows1.size(); i++){
            if (errors.size() >= ERRORS_LIMIT_PER_TABLE){
                return errors;
            }
            if (!findSuitableRow(rows1.get(i), rows2, tolerance)){
                errors.add(
                        new Error(format("Row %s in table %s from file %s was not found in file %s \n %s",
                                i + 1, tableName, file1Path, file2Path, rowToString(rows1.get(i))))
                );
            }
        }
        return errors;
    }

    private static boolean findSuitableRow(Map<String, Object> row1, List<Map<String, Object>> rows2, int tolerance) {
        for (int i = 0; i < rows2.size(); i++){
            Map<String, Object> row2 = rows2.get(i);

            if (row1.size() != row2.size()){
                continue;
            }

            if (rowsMatch(row1, row2, tolerance)){
                rows2.remove(row2);
                return true;
            }
        }

        return false;
    }

    private static boolean rowsMatch(Map<String, Object> row1, Map<String, Object> row2, int tolerance) {
        for (String key : row1.keySet()){
            Object val1 = row1.get(key);
            Object val2 = row2.get(key);

            if (val1 == null && val2 != null ){
                return false;
            }

            if (val1 != null && val2 == null ){
                return false;
            }

            if (val1 == null && val2 == null) continue;

            //Validate numeric values

            if (val1 instanceof Integer && val2 instanceof Integer) {
                int relDiff = 100 * (Math.abs((Integer)val1 - (Integer)val2));
                if (relDiff != 0 && relDiff / Math.max((Integer)val1, (Integer)val2) > tolerance){
                    return false;
                }
            } else if (val1 instanceof Double && val2 instanceof Double) {
                double relDiff = 100 * (Math.abs((Double) val1 - (Double) val2));
                if (relDiff != 0 && relDiff / Math.max((Double) val1, (Double) val2) > tolerance){
                    return false;
                }
            } else if (val1 instanceof Float && val2 instanceof Float) {
                float relDiff = 100 * (Math.abs((Float) val1 - (Float) val2));
                if (relDiff != 0 && relDiff / Math.max((Float) val1, (Float) val2) > tolerance){
                    return false;
                }
            } else if (val1 instanceof Short && val2 instanceof Short) {
                int relDiff = 100 * (Math.abs((Short)val1 - (Short) val2));
                if (relDiff != 0 && relDiff / Math.max((Short)val1, (Short)val2) > tolerance){
                    return false;
                }
            } else if (!val1.toString().equals(val2.toString())){
                return false;
            }

        }
        return true;
    }

    private static String rowToString(Map<String, Object> row) {
        return row.entrySet().stream().map(entry -> format("%s = %s", entry.getKey(), entry.getValue())).
                collect(Collectors.joining(", "));
    }

    private static Error getNumericError(Object val1, Object val2, int row, String columnName, String tableName){
        return new Error(format("Values are different in table %s, row %s, column %s. Got values [%s] and [%s]",
                tableName, row, columnName, val1, val2));
    }

    private static boolean validateTablesListsEqual(Set<String> tables1, Set<String> tables2) {
        if (tables1.size() != tables2.size()){
            return false;
        }
        for (String table : tables1){
            if (tables2.contains(table)) return false;
        }
        return true;
    }

}
