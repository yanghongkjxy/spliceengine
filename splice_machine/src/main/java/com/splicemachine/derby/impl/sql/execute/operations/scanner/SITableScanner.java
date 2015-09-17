package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.splicemachine.constants.FixedSIConstants;
import com.splicemachine.constants.FixedSpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.ExecRowAccumulator;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.dvd.SerializerMap;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.AbstractSkippingScanFilter;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.PackedTxnFilter;
import com.splicemachine.si.impl.TxnFilter;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

//import com.splicemachine.constants.SIConstants;

/**
 * TableScanner which applies SI to generate a row
 * @author Scott Fines
 * Date: 4/4/14
 */
public class SITableScanner<Data> implements StandardIterator<ExecRow>,AutoCloseable{
    private MeasuredRegionScanner<Data> regionScanner;
    private MetricFactory metricFactory;
    private final Scan scan;
    private final ExecRow template;
    private final String tableVersion;
    private final int[] rowDecodingMap;
    private SIFilter<Data> siFilter;
    private EntryPredicateFilter predicateFilter;
    private RowLocation currentRowLocation;
    private final boolean[] keyColumnSortOrder;
    private String indexName;
//    private ByteSlice slice = new ByteSlice();
    private boolean isKeyed = true;
    private KeyIndex primaryKeyIndex;
    private MultiFieldDecoder keyDecoder;
    private final Supplier<MultiFieldDecoder> keyDecoderProvider;
    private ExecRowAccumulator keyAccumulator;
    private int[] keyDecodingMap;
    private FormatableBitSet accessedKeys;
    private final SIFilterFactory filterFactory;
    private ExecRowAccumulator accumulator;
    private final SDataLib dataLib;

    private MergingReader<Data> reader;
    private SerializerMap serializerMap;

    protected SITableScanner(final SDataLib dataLib,
                             MeasuredRegionScanner<Data> scanner,
                             final TransactionalRegion region,
                             final ExecRow template,
                             MetricFactory metricFactory,
                             Scan scan,
                             final int[] rowDecodingMap,
                             final TxnView txn,
                             int[] keyColumnEncodingOrder,
                             boolean[] keyColumnSortOrder,
                             int[] keyColumnTypes,
                             int[] keyDecodingMap,
                             FormatableBitSet accessedPks,
                             String indexName,
                             final String tableVersion,
                             SIFilterFactory filterFactory,
                             TypeProvider keyTypeProvider,
                             SerializerMap serializerMap) {
        this.dataLib = dataLib;
        this.metricFactory=metricFactory;
        this.serializerMap = serializerMap;
        this.scan = scan;
        this.template = template;
        this.rowDecodingMap = rowDecodingMap;
        this.keyColumnSortOrder = keyColumnSortOrder;
        this.indexName = indexName;
        this.regionScanner = scanner;
        this.keyDecodingMap = keyDecodingMap;
        this.accessedKeys = accessedPks;
        this.keyDecoderProvider = getKeyDecoder(accessedPks,keyColumnEncodingOrder, keyColumnTypes,keyTypeProvider);
        this.tableVersion = tableVersion;
        if(filterFactory==null){
            this.filterFactory = new SIFilterFactory<Data>() {
                @Override
                public SIFilter<Data> newFilter(EntryPredicateFilter predicateFilter,
                                                EntryDecoder rowEntryDecoder,
                                                EntryAccumulator accumulator,
                                                boolean isCountStar) throws IOException {
                    HRowAccumulator<Data> hRowAccumulator =new HRowAccumulator<>(dataLib,predicateFilter,
                            rowEntryDecoder,accumulator,
                            isCountStar);
                    return newFilter(predicateFilter, rowEntryDecoder, hRowAccumulator, isCountStar);
                }

                @SuppressWarnings("unchecked")
                @Override
                public SIFilter<Data> newFilter(EntryPredicateFilter predicateFilter,
                                                EntryDecoder rowEntryDecoder,
                                                HRowAccumulator<Data> accumulator,
                                                boolean isCountStar) throws IOException{
                    TxnFilter<Data> txnFilter = region.unpackedFilter(txn);

                    //noinspection unchecked
                    return new PackedTxnFilter<Data>(txnFilter, accumulator){
                        @Override
                        public Filter.ReturnCode doAccumulate(Data dataKeyValue) throws IOException {
                            if (!accumulator.isFinished() && accumulator.isOfInterest(dataKeyValue)) {
                                if (!accumulator.accumulate(dataKeyValue)) {
                                    return Filter.ReturnCode.NEXT_ROW;
                                }
                                return Filter.ReturnCode.INCLUDE;
                            }else return Filter.ReturnCode.INCLUDE;
                        }
                    };
                }
            };
        } else
            this.filterFactory = filterFactory;
    }

    @Override
    public void open() throws StandardException, IOException {

    }

    @SuppressWarnings("unchecked")
    @Override
    public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(reader==null){
            final HRowAccumulator<Data> hRowAccumulator =getAccumulator();
            MergingReader.RowKeyFilter keyFilter = getKeyFilter();
            reader =new MergingReader<>(hRowAccumulator,regionScanner,dataLib,keyFilter,new Supplier<SIFilter<Data>>(){
                @SuppressWarnings("unchecked")
                @Override
                public SIFilter<Data> get(){
                    return getSIFilter(hRowAccumulator);
                }
            },metricFactory);
        }
        template.resetRowArray();
        if(!reader.readNext()){
            currentRowLocation = null;
            return null;
        }
        setRowLocation(reader.currentRowKey());
        return template;
    }


    public long getBytesOutput(){
        if(reader==null) return 0l;
        return reader.getBytesOutput();
    }

    public void recordFieldLengths(int[] columnLengths){
        if(rowDecodingMap!=null) {
            for (int i = 0; i < rowDecodingMap.length; i++) {
                int pos = rowDecodingMap[i];
                if(pos<0) continue;
                columnLengths[pos] = accumulator.getCurrentLength(i);
            }
        }
        if(keyDecodingMap!=null) {
            for (int i = 0; i < keyDecodingMap.length; i++) {
                int pos = keyDecodingMap[i];
                if(pos<0) continue;
                columnLengths[pos] = keyAccumulator.getCurrentLength(i);
            }
        }
    }

    public RowLocation getCurrentRowLocation(){
        return currentRowLocation;
    }


    //TODO -sf- add a nextBatch() method

    @Override
    public void close() throws StandardException, IOException {
        if(keyAccumulator!=null)
            keyAccumulator.close();
        if(siFilter!=null)
            siFilter.getAccumulator().close();
    }

    public TimeView getTime(){
        if(reader==null) return Metrics.noOpTimeView();
        return reader.getTime();
    }

    public long getRowsFiltered(){
        if(reader==null) return 0l;
        return reader.getRowsFiltered();
    }

    public long getRowsVisited() {
        if(reader==null) return 0l;
        return reader.getRowsVisited();
    }

    public void setRegionScanner(MeasuredRegionScanner<Data> scanner){
        if(reader!=null)
            reader.setRegionScanner(scanner);
        else
            this.regionScanner = scanner;
    }


    public long getBytesVisited() {
        if(reader==null) return 0l;
        return reader.getBytesVisited();
    }

    public MeasuredRegionScanner<Data> getRegionScanner() {
        if(reader!=null) return reader.getRegionScanner();
        return regionScanner;
    }

    /*********************************************************************************************************************/
		/*Private helper methods*/
    private Supplier<MultiFieldDecoder> getKeyDecoder(FormatableBitSet accessedPks,
                                                      int[] allPkColumns,
                                                      int[] keyColumnTypes,
                                                      TypeProvider typeProvider) {
        if(accessedPks==null||accessedPks.getNumBitsSet()<=0){
            isKeyed = false;
            return null;
        }

        primaryKeyIndex = getIndex(allPkColumns,keyColumnTypes,typeProvider);

        keyDecoder = MultiFieldDecoder.create();
        return Suppliers.ofInstance(keyDecoder);
    }

    private KeyIndex getIndex(final int[] allPkColumns, int[] keyColumnTypes,TypeProvider typeProvider) {
        return new KeyIndex(allPkColumns,keyColumnTypes, typeProvider);
    }

    private HRowAccumulator<Data> getAccumulator() throws IOException{
        if(accumulator==null){
            predicateFilter= buildInitialPredicateFilter();
            accumulator = ExecRowAccumulator.newAccumulator(predicateFilter,false,template,rowDecodingMap,serializerMap());
        }
        boolean isCountStar = scan.getAttribute(FixedSIConstants.SI_COUNT_STAR)!=null;
        return new HRowAccumulator<>(dataLib,predicateFilter, getRowEntryDecoder(),accumulator, isCountStar);
    }

    @SuppressWarnings("unchecked")
    private SIFilter getSIFilter(HRowAccumulator<Data> accumulator)  {
        if(siFilter==null) {
            boolean isCountStar = scan.getAttribute(FixedSIConstants.SI_COUNT_STAR)!=null;
            try{
                if(predicateFilter==null)
                    predicateFilter=buildInitialPredicateFilter();
                siFilter=filterFactory.newFilter(predicateFilter,getRowEntryDecoder(),accumulator,isCountStar);
            }catch(IOException ioe){
                throw new RuntimeException(ioe);
            }
        }
        return siFilter;
    }

    protected EntryDecoder getRowEntryDecoder() {
        return new EntryDecoder();
    }

    @SuppressWarnings("unchecked")
    private EntryPredicateFilter buildInitialPredicateFilter() throws IOException {
        if(this.scan.getFilter()!=null){
            AbstractSkippingScanFilter skippingScanFilter=Scans.findSkippingScanFilter(this.scan);

            EntryPredicateFilter entryPredicateFilter=EntryPredicateFilter.fromBytes(this.scan.getAttribute(FixedSpliceConstants.ENTRY_PREDICATE_LABEL));
            BitSet checkedColumns=entryPredicateFilter.getCheckedColumns();
            ScopedPredicates<Data> scopedPredicates=new ScopedPredicates(skippingScanFilter,dataLib);
            if(scopedPredicates.isScanWithScopedPredicates()){
                /*
                 * We have to be careful--EMPTY_PREDICATE could have been returned, in which case
                 * setting predicates can cause all kinds of calamituous behavior. To avoid that, when
                 * we have a skipscan filter (e.g. some kind of block scanning like a Batch NLJ) and we
                 * have an empty Predicate Filter to begin with, then we clone the predicate filter to a new
                 * one to avoid contamination.
                 *
                 */
                return new EntryPredicateFilter(checkedColumns,new ObjectArrayList<Predicate>(),entryPredicateFilter.indexReturned());
            }
        }
        return EntryPredicateFilter.fromBytes(this.scan.getAttribute(FixedSpliceConstants.ENTRY_PREDICATE_LABEL));
    }

    protected void setRowLocation(ByteSlice rowKey) throws StandardException {
        if(indexName!=null && template.nColumns() > 0 && template.getColumn(template.nColumns()).getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID){
            /*
			 * If indexName !=null, then we are currently scanning an index,
			 * so our RowLocation should point to the main table, and not to the
			 * index (that we're actually scanning)
			 */
            if (template.getColumn(template.nColumns()).getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID) {
                currentRowLocation = (RowLocation) template.getColumn(template.nColumns());
            } else {
                //TODO -sf- does this block even work?
                byte[] loc =Encoding.decodeBytesUnsortd(rowKey.array(),rowKey.offset(),rowKey.length());
                if(currentRowLocation==null)
                    currentRowLocation = new HBaseRowLocation(loc);
                else
                    currentRowLocation.setValue(loc);
            }
        } else {
            if(currentRowLocation==null)
                currentRowLocation = new HBaseRowLocation(rowKey);
            else
                currentRowLocation.setValue(rowKey);
        }
    }

    private MergingReader.RowKeyFilter getKeyFilter(){
        if(!isKeyed) return MergingReader.RowKeyFilter.NOOPFilter;
        keyAccumulator = ExecRowAccumulator.newAccumulator(predicateFilter,false,
                template,
                keyDecodingMap,
                keyColumnSortOrder,
                accessedKeys,
                serializerMap());
        return new MergingReader.RowKeyFilter(){
            @Override
            public boolean filter(ByteSlice rowKey) throws IOException{
                primaryKeyIndex.reset();
                keyDecoder.set(rowKey.array(),rowKey.offset(),rowKey.length());
                return predicateFilter.match(primaryKeyIndex,keyDecoderProvider,keyAccumulator);
            }

            @Override public boolean applied(){ return keyAccumulator.hasAccumulated(); }
            @Override public void reset(){
                keyAccumulator.reset();
                primaryKeyIndex.reset();
            }
        };
    }

    private SerializerMap serializerMap(){
        if(serializerMap==null)
            serializerMap = VersionedSerializers.forVersion(tableVersion,false);
        return serializerMap;
    }
}
