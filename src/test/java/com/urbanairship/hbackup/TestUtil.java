package com.urbanairship.hbackup;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
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
    
    public static void shutdownMiniDfs(MiniDFSCluster cluster) {
        // This code was mostly copied from HBase 0.90.4 HBaseClusterTestCase.java -DR
        try {
            cluster.shutdown();
        } catch (Exception e) {
            /// Can get a java.lang.reflect.UndeclaredThrowableException thrown
            // here because of an InterruptedException. Don't let exceptions in
            // here be cause of test failure.
        }
        try {
            FileSystem fs = cluster.getFileSystem();
            if (fs != null) {
                fs.close();
            }
            FileSystem.closeAll();
        } catch (IOException e) { }
    }
}
