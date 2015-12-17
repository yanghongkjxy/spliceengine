package com.splicemachine.si;

import com.splicemachine.si.testsetup.SharedStoreHolder;
import com.splicemachine.si.testsetup.TestTransactionSetup;
import org.junit.*;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 8/21/14
 */
public class HBaseTransactionInteractionTest extends TransactionInteractionTest {

    @Override
    @Before
    public void setUp() throws IOException {
        storeSetup = SharedStoreHolder.getHstoreSetup();
        transactorSetup = new TestTransactionSetup(storeSetup, false);
        useSimple = false;
        baseSetUp();
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @Override
    @Ignore("The constraint won't be passed to the server, so the test will spuriously fail")
    public void insertDeleteInsertInsert() throws Exception{
    }
}