/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.jets3t.service.S3Service;
import org.jets3t.service.model.S3Object;
import org.junit.Assert;

import java.io.*;
import java.util.Random;

public abstract class TestUtil {
    private TestUtil() {
        // No instances allowed
    }

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
    
//    public static void runBackup(String source, String dest, Configuration conf) throws Exception {
//        HBackupConfig config = HBackupConfig.forTests(source, dest, conf);
//        new HBackup(config).runWithCheckedExceptions();
//    }
    
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
        
    public static void verifyHdfsContents(FileSystem fs, String path, String contents) throws Exception {
        if(!path.startsWith("/")) {
            // Never treat paths as relative to home direcotry
            path = "/" + path;
        }
        InputStream is = fs.open(new Path(path));
        TestUtil.assertStreamEquals(contents.getBytes(), is);
    }
    
    
    public static void verifyLocalContents(String path, String contents) throws Exception {
        if(!path.startsWith("/")) {
            // Never treat paths as relative to home direcotry
            path = "/" + path;
        }
        InputStream is = new FileInputStream(new File(path));
        TestUtil.assertStreamEquals(contents.getBytes(), is);
    }


    public static void verifyS3Obj(S3Service service, String bucket, String key, byte[] contents)
            throws Exception {
        @SuppressWarnings("unused")
        S3Object[] listing = service.listObjects(bucket);
        S3Object s3Obj = null;
        try {
            s3Obj = service.getObject(bucket, key);
            InputStream is = s3Obj.getDataInputStream(); 
            int objSize = (int)s3Obj.getContentLength();
            if(objSize < 0) {
                Assert.fail("S3 input stream had no bytes available to verify");
            }
            Assert.assertEquals(contents.length, objSize);
            TestUtil.assertStreamEquals(contents, is);
        } finally {
            if(s3Obj != null) {
                s3Obj.closeDataInputStream(); 
            }
        }
    }
    
    public static byte[] getRandomBuf(int size) {
        Random rng = new Random(0);
        byte[] buf = new byte[size];
        rng.nextBytes(buf);
        return buf;
    }
    
    public static String expectedXor(byte[] bytes) {
        byte[] xor = new byte[8];
        for(int i=0; i<Math.min(bytes.length, 8); i++) {
            xor[i%8] = bytes[i];
        }
        for(int i=8; i<bytes.length; i++) {
            xor[i%8] ^= bytes[i];
        }
        
        return new String(Hex.encodeHex(xor));
    }
    
    public static void writeHdfsFile(FileSystem fs, String path, String contents) throws Exception {
        OutputStream os = fs.create(new Path(path), true);
        os.write(contents.getBytes());
        os.close();
    }
    

}
