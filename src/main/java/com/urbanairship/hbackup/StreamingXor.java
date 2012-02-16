package com.urbanairship.hbackup;

import org.apache.commons.codec.binary.Hex;

/**
 * We use the XOR as a simplistic checksum for outgoing data. We can't use traditional checksums
 * because we send the data out of order for multipart transfers.
 */
public class StreamingXor {
    public static final int HASH_BYTES = 8;

    private final byte[] xorSoFar = new byte[HASH_BYTES];
    private final boolean[] haveSeenByteForModulo = new boolean[HASH_BYTES]; // default boolean is false

    protected void updateXor(byte b, long offset) {
        int whichByteToUpdate = (int)(offset % HASH_BYTES);
       
        /* We keep HASH_BYTES bytes of xors, one for each offset modulo HASH_BYTES. The first 
         * time we see a byte for a given modulo, we'll use that as the starting value for the 
         * xor. Later, if we see another byte for that modulo, we'll xor that into the existing 
         * byte.
         *
         * For example:
         *  1. Say we get bytes for offsets 10, 2, and 18, and we're keeping HASH_BYTES=8 hash bytes.
         *  2. Since 10, 2, and 18 are all congruent to 2 modulo 8, they will all affect the same 
         *     byte of the 8 output bytes.
         *  3. When we get the first update for offset 10 with value b1, we'll set xorSoFar[2] to b1
         *     since this is the first time we've seen any updates for modulo value 2.
         *  4. When we later get values b2 at offset 2 and b3 at offset 18, we'll xor b2 and b3
         *     into the existing value b1.
         */
        if(haveSeenByteForModulo[whichByteToUpdate]) {
            xorSoFar[whichByteToUpdate] ^= b;
        } else {
            xorSoFar[whichByteToUpdate] = b;
            haveSeenByteForModulo[whichByteToUpdate] = true;
        }
    }
    
    /**
     * Since xor is commutative and associative, we can combine partial xors to get the same xor
     * that would have resulted from xor'ing all the bytes in order.
     */
    public void update(StreamingXor other) {
        for(int i=0; i<HASH_BYTES; i++) {
            if(other.haveSeenByteForModulo[i]) {
                this.updateXor(other.xorSoFar[i], i);
            }
        }
    }
    
    /**
     * Returns the xor of all the bytes seen so far. Any bytes for which there was no input will
     * be zero. For example, if the stream had a single byte at offset 0, the other 7 bytes of
     * the xor output will be 0.
     */
    public String getXorHex() {
        return new String(Hex.encodeHex(xorSoFar));
    }
}
