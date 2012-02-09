package com.urbanairship.hbackup;

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
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.urbanairship.hbackup.S3Tests.ObjToDelete;

public class MultipartS3Test {
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
    public void multipartTest() throws Exception {
        Assume.assumeTrue(Boolean.valueOf(System.getProperty("hbackup.expensiveTests")));
        
        // Generate six megs of random bytes to upload to S3
        Random rng = new Random(0);
        byte[] sixMegBuf = new byte[6 * 1024 * 1024];
        rng.nextBytes(sixMegBuf);
        
        System.err.println(StringUtils.byteToHexString(MessageDigest.getInstance("MD5").digest(sixMegBuf)));
        
        String filename = "mptest.txt";
        String sourceDir = "from";
        String sinkDir = "to";
        
        String sourceKey = sourceDir + "/" + filename;
        String sinkKey = sinkDir + "/" + filename;
        
        deleteLater(sourceService, sourceBucket, sourceKey);
        deleteLater(sinkService, sinkBucket, sinkKey);
        
        System.err.println("*************************************** Uploading...");
        sourceService.putObject(sourceBucket, new S3Object(sourceKey, sixMegBuf));
        System.err.println("***************************************  Verifying...");
        S3Tests.verifyS3Obj(sourceService, sourceBucket, sourceKey, sixMegBuf);
        System.err.println("*************************************** Done with source file upload");
        
        String sourceUri = "s3://"+sourceBucket+"/"+sourceDir;
        String sinkUri = "s3://"+sinkBucket+"/"+sinkDir; 
        
        SystemConfiguration sysProps = new SystemConfiguration();
        HBackupConfig conf = new HBackupConfig(sourceUri,
                sinkUri,
                2, 
                true, 
                sysProps.getString(HBackupConfig.CONF_SOURCES3ACCESSKEY), 
                sysProps.getString(HBackupConfig.CONF_SOURCES3SECRET), 
                sysProps.getString(HBackupConfig.CONF_SINKS3ACCESSKEY), 
                sysProps.getString(HBackupConfig.CONF_SINKS3SECRET),
                MultipartUtils.MIN_PART_SIZE, // Smallest part size (5MB) will cause multipart upload of 6MB file 
                MultipartUtils.MIN_PART_SIZE, // Use multipart upload if the object is at least this many bytes
                new org.apache.hadoop.conf.Configuration());
        System.err.println("*************************************** Running backup");
        new HBackup(conf).runWithCheckedExceptions();
        System.err.println("*************************************** Verifying backup");
        S3Tests.verifyS3Obj(sinkService, sinkBucket, sinkKey, sixMegBuf);
    }
    
    private void deleteLater(S3Service service, String bucket, String key) {
        toDelete.add(new ObjToDelete(service, bucket, key));
    }
    
    
}
