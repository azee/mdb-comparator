package ru.greatbit.mdb.comparator;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static java.lang.String.format;

public class Comparer {

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
            Error error = tablesEqual(db1, db2, table, tolerance);
            if (error != null){
                error.setPath1(path1);
                error.setPath2(path2);
                errors.add(error);
            }

        }

        return errors;
    }

    private static Error tablesEqual(Database db1, Database db2, String tableName, int tolerance) throws IOException {
        Table table1 = db1.getTable(tableName);
        Table table2 = db2.getTable(tableName);

        String file1 = db1.getFile().getAbsolutePath();
        String file2 = db2.getFile().getAbsolutePath();

        if (table1 == null){
            return new Error(format("Table %s not found in file %s", tableName, db1.getFile().getPath()));
        }

        if (table2 == null){
            return new Error(format("Table %s not found in file %s", tableName, db2.getFile().getPath()));
        }

        if (table1.getRowCount() != table2.getRowCount()){
            return new Error(format("Found different number of rows: %s and %s, for table %s",
                    table1.getRowCount(), table2.getRowCount(), tableName));
        }

        for (int i = 0; i < table1.getRowCount(); i++){
            Map<String, Object> row1 = table1.getNextRow();
            Map<String, Object> row2 = table2.getNextRow();

            if (row1.size() != row2.size()){
                return new Error(format("Found different number of columns: %s and %s, for table %s",
                        table1.getRowCount(), table2.getRowCount(), tableName));
            }


            for (String key : row1.keySet()){
                if (!row2.keySet().contains(key)){
                    return new Error(format("Column %s exists in the %s document but missing in %s for table %s",
                            key, file1, file2, tableName));
                }
            }

            for (String key : row2.keySet()){
                if (!row1.keySet().contains(key)){
                    return new Error(format("Column %s exists in the %s document but missing in %s for table %s",
                            key, file1, file2, tableName));
                }
            }

            for (String key : row1.keySet()){
                Object val1 = row1.get(key);
                Object val2 = row2.get(key);

                if (val1 == null && val2 != null ){
                    return new Error(format(
                            "The value in %s document is null while is not null in %s in table %s, row %s, column %s",
                            file1, file2, tableName, i, key));
                }

                if (val1 != null && val2 == null ){
                    return new Error(format(
                            "The value in %s document is null while is not null in %s in table %s, row %s, column %s",
                            file2, file1, tableName, i, key));
                }

                if (val1 == null && val2 == null) continue;

                //Validate numeric values
                if (val1 instanceof Integer && val2 instanceof Integer &&
                        ((Integer)val1 - (Integer)val2) * 100 > tolerance) {
                    return getNumericError(val1, val2, i, key, tableName);
                }
                if (val1 instanceof Double && val2 instanceof Double &&
                        ((Double)val1 - (Double) val2) * 100 > tolerance) {
                    return getNumericError(val1, val2, i, key, tableName);
                }
                if (val1 instanceof Float && val2 instanceof Float &&
                        ((Float)val1 - (Float) val2) * 100 > tolerance) {
                    return getNumericError(val1, val2, i, key, tableName);
                }
                if (val1 instanceof Short && val2 instanceof Short &&
                        ((Short)val1 - (Short) val2) * 100 > tolerance) {
                    return getNumericError(val1, val2, i, key, tableName);
                }

                if (!val1.toString().equals(val2.toString())){
                    return new Error(format("Values are different in table %s, row %s, column %s. Got values [%s] and [%s] for files %s and %s",
                            tableName, i, key, val1, val2, file1, file2));
                }

            }
        }

        return null;
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
