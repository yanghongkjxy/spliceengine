package com.splicemachine.derby.hbase;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.impl.ActionFactory;
import com.splicemachine.derby.utils.TransactionAdmin;
import com.splicemachine.pipeline.api.Service;
import com.splicemachine.si.impl.TransactionalRegions;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * Coprocessor for starting the splice services on top of HBase.
 *
 * @author John Leach
 */
public class SpliceBaseDerbyCoprocessor {

    public static volatile String regionServerZNode;
    public static volatile String rsZnode;

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed.
     */
    public void start(CoprocessorEnvironment e) {
        RegionServerServices regionServerServices = ((RegionCoprocessorEnvironment) e).getRegionServerServices();

        rsZnode = regionServerServices.getZooKeeper().rsZNode;
        regionServerZNode = regionServerServices.getServerName().getServerName();

        //make sure the factory is correct
        long rollForwardrate = SIConstants.rollForwardRate;
        ActionFactory af = rollForwardrate>0? RollForwardAction.FACTORY:ActionFactory.NOOP_ACTION_FACTORY;
        TransactionalRegions.setActionFactory(af);
        //use the independent write control from the write pipeline
        TransactionalRegions.setTrafficControl(SpliceBaseIndexEndpoint.independentTrafficControl);

        /* We used to only invoke start here if the table was not a hbase meta table, but this method only
         * has an effect once per JVM so it doesn't matter what table this particular coprocessor instance if for. */
        SpliceDriver.driver().registerService(new Service(){
            @Override
            public boolean start(){
                TransactionAdmin.initializeTransactionReader();
                return true;
            }

            @Override
            public boolean shutdown(){
                TransactionAdmin.shutdownTransactionReader();
                return true;
            }
        });
        SpliceDriver.driver().start(regionServerServices);
    }

}