package com.urbanairship.hbackup;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.io.LimitInputStream;

public class XorStreamTest {
    @Test
    public void basicTest() throws Exception {
        // Compute hashes of random buffers from size 0 to 100
        for(int size=0; size<100; size++) {
            byte[] bytes = randomBytes(size);
            Assert.assertEquals(TestUtil.expectedXor(bytes), 
                    streamingXor(bytes, 0, bytes.length).getXorHex());
        }
    }
    
    @Test
    public void chunksTest() throws Exception {
        byte[] bytes = randomBytes(10000);
        final Random rng = new Random();
        
        int offset = 0;
        StreamingXor streamingXor = new StreamingXor();
        while(offset < bytes.length) {
            int bytesRemaining = bytes.length - offset; 
            int chunkSize = Math.min(bytesRemaining, rng.nextInt(20) + 1); // [1..20] inclusive
            
            streamingXor.update(streamingXor(bytes, offset, chunkSize));
            
            offset += chunkSize;
        }
        
        Assert.assertEquals(TestUtil.expectedXor(bytes), streamingXor.getXorHex());
    }
    
    private static byte[] randomBytes(int size) {
        Random rng = new Random(0);
        byte[] bytes = new byte[size];
        rng.nextBytes(bytes);
        return bytes;
    }
    
    private static StreamingXor streamingXor(byte[] bytes, int offset, int len) throws Exception {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        bis.skip(offset);
        InputStream is = new LimitInputStream(bis, len);

        XorInputStream xis = new XorInputStream(is, offset);
        while(xis.read() != -1) { }
        
        return xis.getStreamingXor();
    }

}
