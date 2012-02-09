package com.urbanairship.hbackup;

import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.configuration.SystemConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.utils.MultipartUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class S3Tests {
    /**
     * To actually run this test, the S3 configuration must be given. Something like this
     * will work: 
     * 
     *  -Dhbackup.from.s3AccessKey=myAccessKey1 
     *  -Dhbackup.from.s3Secret=myAccessKey2 
     *  -Dhbackup.to.s3AccessKey=myAccessKey2 
     *  -Dhbackup.to.s3Secret=mySecret2 
     *  -Dhbackup.test.sourceBucket=mySourceBucket 
     *  -Dhbackup.test.destBucket=myDestBucket
     */
    private List<ObjToDelete> toDelete = new ArrayList<ObjToDelete>();
//    private static HBackupConfig conf; 
//    private static AWSCredentials sourceCreds;
//    private static AWSCredentials sinkCreds;
    private static S3Service sourceService;
    private static S3Service sinkService;
    private static String sourceBucket;
    private static String sinkBucket;
    
    @BeforeClass
    public static void skipUnlessS3Configured() throws Exception {
        sourceBucket = System.getProperty("hbackup.test.sourceBucket");  
        sinkBucket = System.getProperty("hbackup.test.destBucket"); 

        Assume.assumeNotNull(System.getProperty(HBackupConfig.CONF_SINKS3ACCESSKEY),
                System.getProperty(HBackupConfig.CONF_SOURCES3ACCESSKEY),
                System.getProperty(HBackupConfig.CONF_SINKS3SECRET),
                System.getProperty(HBackupConfig.CONF_SOURCES3SECRET),
                sourceBucket, sinkBucket);
        
        HBackupConfig throwawayConf = HBackupConfig.forTests("fakesource", "fakeddest");
        sourceService = new RestS3Service(throwawayConf.s3SourceCredentials);
        sinkService = new RestS3Service(throwawayConf.s3SinkCredentials);
    }
    
    /**
     * Delete objects created by the last test case.
     */
    @After
    public void cleanup() throws Exception {
//        for(ObjToDelete obj: toDelete) {
//            obj.s3Service.deleteObject(obj.bucket, obj.key);
//        }
//        toDelete.clear();
    }
    
    @Test
    public void s3BasicTest() throws Exception {
        String contents = "ROFL ROFL LMAO";
        String sourceKey = "from/ponies.txt";
        String sinkKey = "to/ponies.txt";
        sourceService.putObject(sourceBucket, new S3Object(sourceKey, contents.getBytes()));
        deleteLater(sourceService, sourceBucket, sourceKey);
        deleteLater(sinkService, sinkBucket, sinkKey);
        runBackup("s3://"+sourceBucket+"/from", "s3://"+sinkBucket+"/to");
        verifyS3Obj(sinkService, sinkBucket, sinkKey, contents.getBytes());
    }
    
   
    
    public static void verifyS3Obj(S3Service service, String bucket, String key, byte[] contents) 
            throws Exception {
        S3Object s3Obj = service.getObject(bucket, key);
        InputStream is = s3Obj.getDataInputStream(); 
        int objSize = (int)s3Obj.getContentLength();
        if(objSize <= 0) {
            Assert.fail("S3 input stream had no bytes available to verify");
        }
        Assert.assertEquals(contents.length, objSize);
        byte[] buf = new byte[objSize];
        int bytesRead = 0;
        while(bytesRead != objSize) {
            int bytesThisRead = is.read(buf, bytesRead, objSize-bytesRead);
            if(bytesThisRead < 0) {
                Assert.fail();
            }
            bytesRead += bytesThisRead;
        }
        is.close();
        Assert.assertArrayEquals(contents, buf);
    }
    
    public static void runBackup(String from, String to) throws Exception {
        HBackupConfig conf = HBackupConfig.forTests(from, to);
        new HBackup(conf).runWithCheckedExceptions();
    }
    
    
//    private void uploadS3Obj(HBackupConfig conf, S3Service s3Service, String bucket, String key, 
//            String contents) throws Exception {
//        S3Service s3Service = isSource ? sourceService : sinkService;
//        s3Service.putObject(bucket, new S3Object(key, contents.getBytes()));
//        
//    }
    
    private void deleteLater(S3Service service, String bucket, String key) {
        toDelete.add(new ObjToDelete(service, bucket, key));
    }
    
    /**
     * Every time we create an S3 object in a test case, we'll create one of these to remember
     * to delete it later.
     */
    static class ObjToDelete {
        public final S3Service s3Service;
        public final String bucket;
        public final String key;
        
        public ObjToDelete(S3Service s3Service, String bucket, String key) {
            this.s3Service = s3Service;
            this.bucket = bucket;
            this.key = key;
        }
    }
    
    // TODO tests to write:
    // - Multipart (force with low threshold)
    // - Absent source or dest bucket
    // - Other objects not matching "source/"
    // - Overwriting objects
    // - Sink is newer, shouldn't overwrite
    // - No permissions to overwrite
    // - Null credentials in conf object
    // - Bad S3 URL
    // - Dest same as source
    // - Backup source and/or dest are single named file
    // - Copying files with escaped special characters e.g. '/'
    // - Trailing slashes in source and dest
}
