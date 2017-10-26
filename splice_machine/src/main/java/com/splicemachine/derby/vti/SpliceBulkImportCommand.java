package com.splicemachine.derby.vti;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Created by avagarwa on 1/11/2017.
 */
public class SpliceBulkImportCommand //extends PipelineCommand {
{
    private static final Logger LOG = Logger.getLogger(SpliceBulkImportCommand.class);
    private static final String VTI_CLASS_NAME = "com.visa.data.splice.RowVTI";

    private String jdbcUrl;
    private String tableName;
    private int batchSize;

    private String insertSql; //not a json element

    public Dataset<Row> apply(Dataset<Row> dataFrame) {
        JavaRDD<Row> records = dataFrame.toJavaRDD();

        if(insertSql == null || insertSql.isEmpty()) {
            try {
                //NOTE: Assuming dataFrame.schema doesn't change during runtime.
                //TODO validate above assumption later
                StructType schema = dataFrame.schema();
                insertSql = getVTISyntaxForType(Arrays.asList(schema.fieldNames()));
                LOG.info("SPLICE VTI QUERY:"+insertSql);
            } catch (SQLException e) {
                throw new RuntimeException("Could not make jdbc connection : " + jdbcUrl, e);
            }
        }
        records.foreachPartition(new SavePartitionWithVTI(jdbcUrl, insertSql, batchSize));
        return dataFrame;
    }

    /**
     * This method is written to overcome the performance issue associated with creating below
     * instance on splice machine side. It takes ~15mins to creating this obj when we have ~1000
     * columns.
     *
     * ColumnInfo columnInfo = new ColumnInfo(con, tuple[0], tuple[1], columnList);
     */
    private String getVTISyntaxForType(List<String> columnList)
            throws SQLException {
        String columnsInQuery;
        String columnsImportInQuery;
        Connection con = null;
        StringBuilder columnsBuilder = new StringBuilder();
        StringBuilder columnsImportBuilder = new StringBuilder();
        String[] tuple = this.tableName.split("\\.");
        String schema = tuple[0];
        String table = tuple[1];
        try {
            con = DriverManager.getConnection(jdbcUrl);
            DatabaseMetaData databaseMetaData = con.getMetaData();
            ResultSet rs = databaseMetaData.getColumns(null, schema, table, null);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                String colName = null;
                String size = null;
                String type = null;
                String decimalDigits = null;

                for (int i = 1; i <= columnsNumber; i++) {
                    LOG.trace(rsmd.getColumnName(i) + ":" + rs.getString(i));
                    if (rsmd.getColumnName(i).equalsIgnoreCase("COLUMN_NAME")) {
                        colName = rs.getString(i);
                    }
                    if (rsmd.getColumnName(i).equalsIgnoreCase("COLUMN_SIZE")) {
                        size = rs.getString(i);
                    }
                    if (rsmd.getColumnName(i).equalsIgnoreCase("TYPE_NAME")) {
                        type = rs.getString(i);
                    }
                    if (rsmd.getColumnName(i).equalsIgnoreCase("DECIMAL_DIGITS")) {
                        decimalDigits = rs.getString(i);
                    }
                }

                if (colName != null && size != null && type != null) {
                    if (columnList.contains(colName)) {
                        columnsBuilder.append("\"").append(colName).append("\"").append(",");
                        columnsImportBuilder.append("\"").append(colName).append("\"").append(" ");
                        if (type.equalsIgnoreCase("char")
                                || type.equalsIgnoreCase("varchar")
                                || type.equalsIgnoreCase("decimal")) {
                            columnsImportBuilder.append(type).append("(").append(size);
                            if (decimalDigits != null && decimalDigits.trim() != null && !decimalDigits
                                    .trim().equalsIgnoreCase("0")) {
                                columnsImportBuilder.append(",").append(decimalDigits);
                            }
                            columnsImportBuilder.append(")").append(",");
                        } else {
                            columnsImportBuilder.append(type).append(",");
                        }
                    } else {
                        LOG.info("Skipping column as its not present in filter:" + colName);
                    }
                } else {
                    throw new RuntimeException(
                            "error due to null values:" + colName + ", " + size + ", " + type);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not make jdbc connection : " + jdbcUrl, e);
        } finally {
            if (con != null) {
                con.close();
            }
        }

        if (columnsBuilder.length() > 0) {
            //chop the last comma separator
            columnsBuilder.setLength(columnsBuilder.length() - 1);
            columnsInQuery = columnsBuilder.toString();
            LOG.info("columnsInQuery:" + columnsInQuery);
        } else {
            throw new RuntimeException(
                    "error building column substitutions for:" + schema + "." + table);
        }

        if (columnsImportBuilder.length() > 0) {
            //chop the last comma separator
            columnsImportBuilder.setLength(columnsImportBuilder.length() - 1);
            columnsImportInQuery = columnsImportBuilder.toString();
            LOG.info("columnsImportInQuery:" + columnsImportInQuery);
        } else {
            throw new RuntimeException(
                    "error building column substitutions for:" + schema + "." + table);
        }

        return "INSERT INTO " + this.tableName + "(" + columnsInQuery + ") \n" +
                //"--splice-properties useSpark=true, insertMode=INSERT, badRecordsAllowed=-1 \n" +
                //"--splice-properties insertMode=INSERT, skipConflictDetection=true, skipSampling=true, skipWAL=true \n" +
                //"--splice-properties useSpark=true \n" +
                "--splice-properties insertMode=INSERT, skipConflictDetection=true, skipSampling=true\n" +
                " SELECT " + columnsInQuery + " from " +
                "new " + VTI_CLASS_NAME + " (?) " + " AS importVTI (" + columnsImportInQuery + ")";
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

}
