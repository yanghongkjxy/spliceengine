package com.splicemachine.derby.utils;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.actions.ActiveTransactionReader;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.StreamException;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceUtilities;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Utility for Vacuuming Splice.
 *
 * @author Scott Fines
 * Date: 3/19/14
 */
public class Vacuum {
    private static final Logger LOG=Logger.getLogger(Vacuum.class);

		private final Connection connection;
		private final HBaseAdmin admin;

		public Vacuum(Connection connection) throws SQLException {
				this.connection = connection;
				try {
						this.admin = SpliceUtilities.getAdmin();
				} catch (Exception e) {
						throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
				} 
		}

    public List<VacuumStats> vacuumTable(String schemaName,String tableName) throws StandardException{
        if(schemaName==null)
            throw ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException(schemaName);
        schemaName =schemaName.toUpperCase();
        if(tableName==null)
            throw ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException(tableName);
        tableName = tableName.toUpperCase();

        TableDescriptor td = AdminUtilities.verifyTableExists(connection,schemaName,tableName);

        long mat = TransactionAdmin.minimumActiveTimestamp(true);

        ConglomerateDescriptorList congloms=td.getConglomerateDescriptorList();
        List<JobFuture> jobFutures = new ArrayList<>(congloms.size());
        List<HTableInterface> toClose = new ArrayList<>(congloms.size());
        List<VacuumStats> vacStats = new ArrayList<>(congloms.size());
        JobScheduler<CoprocessorJob> jobScheduler=SpliceDriver.driver().getJobScheduler();
        try{
            for(ConglomerateDescriptor cd : congloms){
                long cId=cd.getConglomerateNumber();
                HTableInterface table=SpliceAccessManager.getHTable(Long.toString(cId).getBytes());
                VacuumTableJob vtj=new VacuumTableJob(table,cId,mat);
                try{
                    jobFutures.add(jobScheduler.submit(vtj));
                }catch(ExecutionException e){
//                    for(JobFuture future:jobFutures){
//                        try{
//                            future.cancel();
//                        }catch(ExecutionException e1){
//                            LOG.warn("Unexpected exception encountered while cancelling a job",e1.getCause());
//                        }
//                    }
                    throw Exceptions.parseException(e);
                }
                toClose.add(table);
            }
            Throwable t = null;
            int i =0;
            for(JobFuture future : jobFutures){
                try{
                    try{
                        future.completeAll(null);
                    }finally{
                        future.cleanup();
                    }
//                    try{
//                        if(t==null)
//                            future.completeAll(null);
//                        else
//                            future.cancel();
//                    }finally{
//                        future.cleanup();
//                    }

                    JobStats jobStats=future.getJobStats();
                    List<TaskStats> taskStats=future.getJobStats().getTaskStats();
                    long totalRowsRead = 0l;
                    long totalRowsWritten = 0l;
                    long time = jobStats.getTotalTime();
                    for(TaskStats ts:taskStats){
                        totalRowsRead+=ts.getTotalRowsProcessed();
                        totalRowsWritten+=ts.getTotalRowsWritten();
                    }
                    vacStats.add(new VacuumStats(congloms.get(i).getConglomerateNumber(),time,totalRowsRead,totalRowsWritten));
                }catch(InterruptedException e){
                    t = e;
                }catch(ExecutionException e){
                    t = e.getCause();
                }
            }
            if(t!=null) throw Exceptions.parseException(t);
        }finally{
            for(HTableInterface t:toClose){
               Closeables.closeQuietly(t);
            }
        }
        return vacStats;
    }

    public void vacuumConglomerates() throws SQLException{
        ensurePriorTransactionsComplete();

        //get all the conglomerates from sys.sysconglomerates
        PreparedStatement ps=null;
        ResultSet rs=null;
        LongOpenHashSet activeConglomerates=LongOpenHashSet.newInstance();
        try{
            ps=connection.prepareStatement("select conglomeratenumber from sys.sysconglomerates");

            rs=ps.executeQuery();

            while(rs.next()){
                activeConglomerates.add(rs.getLong(1));
            }
        }finally{
            if(rs!=null)
                rs.close();
            if(ps!=null)
                ps.close();
        }

        //get all the tables from HBaseAdmin
        try{
            HTableDescriptor[] hTableDescriptors=admin.listTables();

            for(HTableDescriptor table : hTableDescriptors){
                try{
                    long tableConglom=Long.parseLong(Bytes.toString(table.getName()));
                    if(tableConglom<1168l) continue; //ignore system tables
                    if(!activeConglomerates.contains(tableConglom)){
                        SpliceUtilities.deleteTable(admin,table);
                    }
                }catch(NumberFormatException nfe){
                                        /*
										 * This is either TEMP, TRANSACTIONS, SEQUENCES, or something
										 * that's not managed by splice. Ignore it
										 */
								}
						}
				} catch (IOException e) {
						throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
				}
		}

		/*
		 * We have to make sure that all prior transactions complete. Once that happens, we know that the worldview
		 * of all outstanding transactions is the same as ours--so if a conglomerate doesn't exist in sysconglomerates,
		 * then it's not useful anymore.
		 */
		private void ensurePriorTransactionsComplete() throws SQLException {
				EmbedConnection embedConnection = (EmbedConnection)connection;

        TransactionController transactionExecute = embedConnection.getLanguageConnection().getTransactionExecute();
        TxnView activeStateTxn = ((SpliceTransactionManager) transactionExecute).getActiveStateTxn();


				//wait for all transactions prior to us to complete, but only wait for so long
				try{
						long activeTxn = waitForConcurrentTransactions(activeStateTxn);
						if(activeTxn>0){
								//we can't do anything, blow up
								throw PublicAPI.wrapStandardException(
												ErrorState.DDL_ACTIVE_TRANSACTIONS.newException("VACUUM", activeTxn));
						}

				}catch(StandardException se){
						throw PublicAPI.wrapStandardException(se);
				}
		}

    private long waitForConcurrentTransactions(TxnView txn) throws StandardException {
        ActiveTransactionReader reader = new ActiveTransactionReader(0l,txn.getTxnId(),null);
        long timeRemaining = SpliceConstants.ddlDrainingMaximumWait;
        long pollPeriod = SpliceConstants.pause;
        int tryNum = 1;
        long activeTxn;

        try {
            do {
                activeTxn = -1l;

                TxnView next;
                try (Stream<TxnView> activeTransactions = reader.getActiveTransactions(10)){
                    while((next = activeTransactions.next())!=null){
                        long txnId = next.getTxnId();
                        if(txnId!=txn.getTxnId()){
                            activeTxn = txnId;
                            break;
                        }
                    }
                }

                if(activeTxn<0) return activeTxn; //no active transactions

                long time = System.currentTimeMillis();
					
					try {
						Thread.sleep(Math.min(tryNum*pollPeriod,timeRemaining));
					} catch (InterruptedException e) {
						throw new IOException(e);
					}
					timeRemaining-=(System.currentTimeMillis()-time);
					tryNum++;
				} while (timeRemaining>0);
			} catch (IOException | StreamException e) {
				throw Exceptions.parseException(e);
			}

        return activeTxn;
		} // end waitForConcurrentTransactions

		public void shutdown() throws SQLException {
				try {
						admin.close();
				} catch (IOException e) {
						throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
				}
		}
}
