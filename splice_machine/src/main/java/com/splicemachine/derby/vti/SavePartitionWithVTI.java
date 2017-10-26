package com.splicemachine.derby.vti;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.util.LongAccumulator;

/**
 * Created by avagarwa on 1/11/2017.
 */
public class SavePartitionWithVTI implements VoidFunction<Iterator<Row>>, Serializable {
    private static final long serialVersionUID = -4407482578122953684L;
    private static final Logger LOG = Logger.getLogger(SavePartitionWithVTI.class);

    private final String jdbcUrl;
    private final String insertSql;
    private final int batchSize;

    //public final LongAccumulator spliceLatencyAccumulator;

    public SavePartitionWithVTI(String jdbcUrl, String insertSql, int batchSize) {
        this.jdbcUrl = jdbcUrl;
        this.insertSql = insertSql;
        this.batchSize = batchSize;
        //this.spliceLatencyAccumulator = ContextWrapper.getJavaSparkContext().sc().longAccumulator("SpliceVTI-ElapsedTimeInMillis");
    }

    @Override
    public void call(Iterator<Row> rowIterator) throws Exception {
        Connection conn = DriverManager.getConnection(jdbcUrl);

        List<Row> records = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        while (rowIterator.hasNext()) {
            records.add(rowIterator.next());
            if (records.size() >= batchSize) {
                executeStatement(records, conn);
                records.clear();
            }
        }
        executeStatement(records, conn);
        conn.close();
        long endTime = System.currentTimeMillis();
        //spliceLatencyAccumulator.setValue((endTime - startTime));
    }

    private void executeStatement(List<Row> records, Connection conn) throws SQLException, IOException {
        PreparedStatement ps = conn.prepareStatement(insertSql);

        int numRecords = records.size();
        if (numRecords > 0) {
            byte[] bytes = serialize(records);
            ps.setBytes(1, bytes);
            try {
                ps.executeUpdate();
            } catch (Exception e) {
                LOG.error("Exception inserting data:" + e.getMessage(), e);
            }
        }
    }

    /**
     * Convert the Dataset<Row> object to a byte array to send over as a BLOB to the VTI
     *
     * @param obj
     * @return
     * @throws IOException
     */
    public byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = null;
        ObjectOutputStream o = null;
        byte[] bytes = null;
        try {
            b = new ByteArrayOutputStream();
            o = new ObjectOutputStream(b);
            o.writeObject(obj);
            bytes = b.toByteArray();
        } catch (Exception e) {
            LOG.error("Exception serializing the object.", e);
            throw e;
        } finally {
            if (o != null) try {
                o.close();
            } catch (Exception ignored) {
            }
            ;
            if (b != null) try {
                b.close();
            } catch (Exception ignored) {
            }
            ;
        }
        return bytes;

    }
}

