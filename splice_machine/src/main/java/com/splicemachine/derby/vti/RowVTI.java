package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DateTimeDataValue;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.SpliceDateFunctions;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.spark_project.guava.collect.FluentIterable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Blob;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * Created by avagarwa on 1/11/2017.
 */
public class RowVTI implements DatasetProvider, VTICosting {
    private static final Logger LOG = Logger.getLogger(RowVTI.class);

    private OperationContext operationContext;

    private List<Row> records = null;

    private String timeFormat;

    private String dateTimeFormat;

    private String timestampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private transient Calendar calendar;

    private transient DateTimeFormatter parser;

    public RowVTI(Blob recordBlob) throws SQLException, IOException, ClassNotFoundException {
        if (recordBlob != null)
            this.records = (List<Row>) getBlobResults(recordBlob);

        parser = DateTimeFormat.forPattern(timestampFormat);

    }


    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation spliceOperation, DataSetProcessor dataSetProcessor, ExecRow execRow) throws StandardException {
        operationContext = dataSetProcessor.createOperationContext(spliceOperation);

        FluentIterable<ExecRow> iterable = null;

        try {
            int numRcds = this.records == null ? 0 : this.records.size();
            if (numRcds > 0) {
                iterable = FluentIterable.from(records).transform(row -> {
                    return getRow(execRow,row);
                });
            }
        } catch (Exception e) {
            LOG.error("Exception processing RowVTI", e);
        } finally {
            operationContext.popScope();
        }
        return new ControlDataSet<>(iterable.iterator());
    }


    /**
     * Converts the blog back into an Iterator<Row>
     *
     * @param data
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public Object getBlobResults(Blob data) throws SQLException, ClassNotFoundException, IOException {
        Object obj = null;

        if (data == null) {
            return obj;
        }

        long length = data.length();
        if (length > 0) {
            byte[] bytes = data.getBytes(Long.valueOf(1), Integer.valueOf((length) + ""));
            obj = deserialize(bytes);
        }

        //LOG.error("Blob results deserialized:" + obj);
        LOG.error("Blob type:" + obj.getClass().getName());

        return obj;
    }


    /**
     * Deserializes the object
     *
     * @param bytes
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = null;
        ObjectInputStream o = null;
        Object obj = null;

        try {
            b = new ByteArrayInputStream(bytes);
            o = new ObjectInputStream(b);
            o.close();
            obj = o.readObject();

        } finally {
            if (o != null) try {
                o.close();
            } catch (Exception e) {
            };
            if (b != null) try {
                b.close();
            } catch (Exception e) {
            };
        }
        return obj;
    }

    /**
     * This will dynamically determine what the response should be
     *
     *
     * @return
     * @throws Exception
     */
    public ExecRow getRow(ExecRow execRow, Row dataFrameRow) {
        try {

            //We need to retrieve the keys and values from the Hashmpa object
            //part of the reason we can't do it on the fly is that there is
            //a case sensitivity issue between was comes in the JSON vs
            //what the database column name.

            ExecRow returnRow = execRow.getClone();

            for (int i = 1; i <= returnRow.nColumns(); i++) {
                DataValueDescriptor dvd = returnRow.getColumn(i);
                int type = dvd.getTypeFormatId();

                boolean isNull = dataFrameRow.isNullAt(i-1);
                Object objVal = dataFrameRow.apply(i-1);
                String value = "";
                if(objVal != null)
                    value = objVal.toString();

                if (value != null && (value.equals("null") || value.equals("NULL") || value.isEmpty()))
                    value = null;
                if (type == StoredFormatIds.SQL_TIME_ID) {
                    if (calendar == null)
                        calendar = new GregorianCalendar();
                    if (timeFormat == null || value == null) {
                        ((DateTimeDataValue) dvd).setValue(value, calendar);
                    } else
                        dvd.setValue(SpliceDateFunctions.TO_TIME(value, timeFormat), calendar);
                } else if (type == StoredFormatIds.SQL_TIMESTAMP_ID) {
                    if (calendar == null)
                        calendar = new GregorianCalendar();
                    if (timestampFormat == null || value == null)
                        ((DateTimeDataValue) dvd).setValue(value, calendar);
                    else
                        dvd.setValue(new Timestamp(parser.withOffsetParsed().parseDateTime(value).getMillis()), calendar);
                } else if (type == StoredFormatIds.SQL_DATE_ID) {
                    if (calendar == null)
                        calendar = new GregorianCalendar();
                    if (dateTimeFormat == null || value == null)
                        ((DateTimeDataValue) dvd).setValue(value, calendar);
                    else
                        dvd.setValue(SpliceDateFunctions.TO_DATE(value, dateTimeFormat), calendar);
                } else {
                    dvd.setValue(value);
                }
            }
            return returnRow;
        } catch (Exception e) {
            LOG.error("Exception building row", e);
            throw new RuntimeException(e);
        }
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLException("not supported");
    }

    @Override
    public OperationContext getOperationContext() {
        return this.operationContext;
    }

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {
        return 0;
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment) throws SQLException {
        return false;
    }
}

