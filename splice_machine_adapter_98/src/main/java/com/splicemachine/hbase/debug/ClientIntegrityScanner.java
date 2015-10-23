package com.splicemachine.hbase.debug;

import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.impl.DefaultCellTypeParser;
import com.splicemachine.si.impl.MemoryTimestampSource;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.si.impl.store.CompletedTxnCacheSupplier;
import com.splicemachine.si.impl.txnclient.CoprocessorTxnStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/7/15
 */
public class ClientIntegrityScanner extends Configured implements Tool{


    @Override
    public int run(String[] args) throws Exception{
        int argPos=0;
        String table = null;
        long minVersion = -1;
        long maxVersion = -1;
        while(argPos<args.length){
           switch(args[argPos]){
               case "-h":
                   printHelp();
                   return 1;
               case "-t":
                   argPos++;
                   if(argPos==args.length){
                       System.out.println("no tables specified");
                       printHelp();
                       return 2;
                   }
                   table=args[argPos];
                   argPos++;
                   break;
               case "-startTime":
                   argPos++;
                   if(argPos==args.length){
                       System.out.println("Invalid arg: startTime");
                       printHelp();
                       return 2;
                   }
                   minVersion = Long.parseLong(args[argPos]);
                   argPos++;
                   break;
               case "-endTime":
                   argPos++;
                   if(argPos==args.length){
                       System.out.println("Invalid arg: endTime");
                       printHelp();
                       return 2;
                   }
                   maxVersion = Long.parseLong(args[argPos]);
                   argPos++;
                   break;
               default:
                   System.out.println("Unknown arg: "+args[argPos]);
                   printHelp();
                   return 2;
           }
        }
        if(table==null){
            System.out.println("No table specified!");
            printHelp();
            return 3;
        }

        //tables can be comma-separated
        String[] tables = table.split(",");
        Configuration conf = getConf();
        HTableInterfaceFactory tif = new HTableFactory();

        TxnSupplier txnSupplier = newTxnSupplier(tif,conf);

        for(String t:tables){
            System.out.println("Table: "+t);
            System.out.println("-----------");
            validate(t,conf,tif,txnSupplier,minVersion,maxVersion);
            System.out.println("-----------");
        }

        return 0;
    }

    private void printHelp(){
        System.out.println("Usage: "+this.getClass().getSimpleName()+" -t <tables>");
    }

    private TxnSupplier newTxnSupplier(HTableInterfaceFactory tif,Configuration conf){
        TimestampSource ts = new MemoryTimestampSource(0l);
        CoprocessorTxnStore coprocessorTxnStore=new CoprocessorTxnStore(tif,ts,null,conf);
        CompletedTxnCacheSupplier completedTxns = new CompletedTxnCacheSupplier(coprocessorTxnStore,1<<14,1);
        ActiveTxnCacheSupplier aSupplier = new ActiveTxnCacheSupplier(completedTxns,1<<14);
        coprocessorTxnStore.setCache(aSupplier);
        return aSupplier;
    }

    private void validate(String table,Configuration conf,HTableInterfaceFactory tif,TxnSupplier supplier,long minVersion,long maxVersion) throws IOException{
        RowIntegrityValidator riv = new RowIntegrityValidator(supplier,DefaultCellTypeParser.INSTANCE);
        try(HTableInterface t = tif.createHTableInterface(conf,table.trim().getBytes())){
            ResultScanner rs = t.getScanner(newScan(minVersion,maxVersion));
            Result r ;
            while((r=rs.next())!=null){
                riv.validate(r);
                riv.rowDone();
            }
        }
        riv.printSummaryInformation();
    }

    private Scan newScan(long minVersion,long maxVersion) throws IOException{
        Scan scan = new Scan();
        scan.setMaxVersions();
        if(maxVersion>0){
            minVersion = Math.max(0,minVersion);
            scan.setTimeRange(minVersion,maxVersion);
        }else if(minVersion>0){
           scan.setTimeRange(minVersion,Long.MAX_VALUE);
        }
        return scan;
    }

    public static void main(String...args) throws Exception{
        System.exit(ToolRunner.run(null,new ClientIntegrityScanner(),args));
    }
}
