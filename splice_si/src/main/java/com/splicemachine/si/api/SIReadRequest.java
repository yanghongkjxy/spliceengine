package com.splicemachine.si.api;

/**
 * @author Scott Fines
 *         Date: 10/12/15
 */
public enum SIReadRequest{
    /**
     * Used when you want to manage SI yourself
     */
    NO_SI((byte)0x00),
    /**
     * Used when you want to resolve transactionality, but don't want to perform any data manipulations
     */
    SI((byte)0x01),
    /**
     * Used when you want to resolve transactionally AND you want to pack the data together
     */
    SI_PACKED((byte)0x02);

    private final byte data;
    private final byte[] encoded;

    SIReadRequest(byte data){
        this.data=data;
        this.encoded = new byte[]{data};
    }

    public byte[] encode(){ return encoded;}

    public static SIReadRequest readRequest(byte[] dataElem){
        if(dataElem==null) return NO_SI;

        switch(dataElem[0]){
            case 0x00:
                return NO_SI;
            case 0x01:
                return SI;
            case 0x02:
                return SI_PACKED;
            default:
                throw new AssertionError("Programmer error: unexpected data elem type: "+ dataElem[0]);
        }
    }
}
