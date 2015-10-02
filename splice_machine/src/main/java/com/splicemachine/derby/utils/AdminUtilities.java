package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.pipeline.exception.ErrorState;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 10/2/15
 */
public class AdminUtilities{
    private AdminUtilities(){}

    public static IteratorNoPutResultSet wrapResults(EmbedConnection conn,
                                                     List<ExecRow> rows,
                                                     ResultColumnDescriptor[] columns) throws StandardException {
        Activation lastActivation = conn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columns, lastActivation);
        resultsToWrap.openCore();
        return resultsToWrap;
    }

    public static ExecRow buildOutputTemplateRow(ResultColumnDescriptor[] columns) throws StandardException {
        ExecRow outputRow = new ValueRow(columns.length);
        DataValueDescriptor[] dvds = new DataValueDescriptor[columns.length];
        for (int i = 0; i < dvds.length; i++) {
            dvds[i] = columns[i].getType().getNull();
        }
        outputRow.setRowArray(dvds);
        return outputRow;
    }

    public static TableDescriptor verifyTableExists(Connection conn, String schema, String table) throws StandardException{
        LanguageConnectionContext lcc = ((EmbedConnection) conn).getLanguageConnection();
        DataDictionary dd = lcc.getDataDictionary();
        SchemaDescriptor schemaDescriptor = getSchemaDescriptor(schema, lcc, dd);
        TableDescriptor tableDescriptor = dd.getTableDescriptor(table, schemaDescriptor, lcc.getTransactionExecute());
        if (tableDescriptor == null)
            throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schema + "." + table);

        return tableDescriptor;
    }

    public static SchemaDescriptor getSchemaDescriptor(String schema,
                                                        LanguageConnectionContext lcc,
                                                        DataDictionary dd) throws StandardException {
        SchemaDescriptor schemaDescriptor = dd.getSchemaDescriptor(schema, lcc.getTransactionExecute(), true);
        if (schemaDescriptor == null)
            throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schema);
        return schemaDescriptor;
    }

    public static String getCurrentSchema() throws SQLException {
        EmbedConnection connection = (EmbedConnection) SpliceAdmin.getDefaultConn();
        return getCurrentSchema(connection);
    }

    public static String getCurrentSchema(EmbedConnection connection) throws SQLException {
        LanguageConnectionContext lcc = connection.getLanguageConnection();
        String schema = lcc.getCurrentSchemaName();

        return schema;

    }
}
