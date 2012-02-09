package com.urbanairship.hbackup;

import java.io.InputStream;

import org.junit.Assert;

public abstract class TestUtil {
    private TestUtil() {
        // No instances allowed
    }
    
    /**
     * Only use this for testing. Hackish.
     */
    public static void assertStreamEquals(byte[] expectedContents, InputStream is) throws Exception{
        byte[] buf = new byte[expectedContents.length];
        int bytesRead = 0;
        while(bytesRead != expectedContents.length) {
            int bytesThisRead = is.read(buf, bytesRead, expectedContents.length-bytesRead);
            if(bytesThisRead < 0) {
                Assert.fail();
            }
            bytesRead += bytesThisRead;
        }
        is.close();
        Assert.assertArrayEquals(expectedContents, buf);
    }
    
    public static void runBackup(String source, String dest) throws Exception {
        HBackupConfig conf = HBackupConfig.forTests(source, dest);
        new HBackup(conf).runWithCheckedExceptions();
    }
}
