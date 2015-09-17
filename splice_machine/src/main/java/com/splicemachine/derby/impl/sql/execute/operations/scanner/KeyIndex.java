package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.storage.Indexed;

/**
 * @author Scott Fines
 *         Date: 9/28/15
 */
class KeyIndex implements Indexed{
    private final int[] allPkColumns;
    private int position = 0;

    private final boolean[] sF;
    private final boolean[] fF;
    private final boolean[] dF;

    KeyIndex(int[] allPkColumns,
                     int[] keyColumnTypes,
                     TypeProvider typeProvider) {
        this.allPkColumns = allPkColumns;
        sF = new boolean[keyColumnTypes.length];
        fF = new boolean[keyColumnTypes.length];
        dF = new boolean[keyColumnTypes.length];

        for(int i=0;i<sF.length;i++){
            sF[i] = typeProvider.isScalar(keyColumnTypes[i]);
            fF[i] = typeProvider.isFloat(keyColumnTypes[i]);
            dF[i] = typeProvider.isDouble(keyColumnTypes[i]);
        }
    }

    @Override public int nextSetBit(int currentPosition) {
        if(position>=allPkColumns.length)
            return -1;
        int pos =position;
        position++;
        return pos;
    }

    @Override public boolean isScalarType(int currentPosition) { return sF[currentPosition]; }
    @Override public boolean isDoubleType(int currentPosition) { return dF[currentPosition]; }
    @Override public boolean isFloatType(int currentPosition)  { return fF[currentPosition]; }

    @Override public int getPredicatePosition(int position) { return allPkColumns[position]; }

    void reset(){
        position=0;
    }
}
