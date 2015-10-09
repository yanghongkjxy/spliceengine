package com.splicemachine.constants;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Splice Constants which are not affected by configuration. Mainly this is useful to avoid construction of a Configuration
 * in testing.
 *
 * @author Scott Fines
 *         Date: 9/16/15
 */
public class FixedSpliceConstants{

    // Splice Internal Tables
    public static final String TEMP_TABLE = "SPLICE_TEMP";
    public static final String TEST_TABLE = "SPLICE_TEST";
    public static final String TRANSACTION_TABLE = "SPLICE_TXN";
    public static final String TENTATIVE_TABLE = "TENTATIVE_DDL";
    public static final int TRANSACTION_TABLE_BUCKET_COUNT = 16; //must be a power of 2
    public static final String CONGLOMERATE_TABLE_NAME = "SPLICE_CONGLOMERATE";
    public static final String SEQUENCE_TABLE_NAME = "SPLICE_SEQUENCES";
    public static final String RESTORE_TABLE_NAME = "SPLICE_RESTORE";
    public static final String SYSSCHEMAS_CACHE = "SYSSCHEMAS_CACHE";
    public static final String SYSSCHEMAS_INDEX1_ID_CACHE = "SYSSCHEMAS_INDEX1_ID_CACHE";
    public static final String[] SYSSCHEMAS_CACHES = {SYSSCHEMAS_CACHE,SYSSCHEMAS_INDEX1_ID_CACHE};

    public static byte[] TEMP_TABLE_BYTES = Bytes.toBytes(TEMP_TABLE);
    public static final byte[] TRANSACTION_TABLE_BYTES = Bytes.toBytes(TRANSACTION_TABLE);
    public static final byte[] TENTATIVE_TABLE_BYTES = Bytes.toBytes(TENTATIVE_TABLE);
    public static final byte[] CONGLOMERATE_TABLE_NAME_BYTES = Bytes.toBytes(CONGLOMERATE_TABLE_NAME);
    public static final byte[] SEQUENCE_TABLE_NAME_BYTES = Bytes.toBytes(SEQUENCE_TABLE_NAME);
    public static final byte[] RESTORE_TABLE_NAME_BYTES = Bytes.toBytes(RESTORE_TABLE_NAME);

    // The column in which splice stores encoded/packed user data.
    public static final byte[] PACKED_COLUMN_BYTES = Bytes.toBytes("7");

    public static final byte[] DEFAULT_FAMILY_BYTES = Bytes.toBytes("V");

    public static final String SI_PERMISSION_FAMILY = "P";

    //TEMP Table task column--used for filtering out failed tasks from the temp
    //table

    // Default Constants
    public static final String SUPPRESS_INDEXING_ATTRIBUTE_NAME = "iu";
    public static final byte[] SUPPRESS_INDEXING_ATTRIBUTE_VALUE = new byte[]{};
    public static final String CHECK_BLOOM_ATTRIBUTE_NAME = "cb";
    public static final String SPLICE_DB = "splicedb";
    public static final String SPLICE_USER = "SPLICE";

    public static final String ENTRY_PREDICATE_LABEL= "p";
    public static final byte[] EMPTY_BYTE_ARRAY= new byte[]{};
}
