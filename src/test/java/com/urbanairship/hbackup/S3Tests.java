package com.urbanairship.hbackup;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.configuration.SystemConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.utils.MultipartUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
public class S3Tests {
    public static final String PROP_CLEARBUCKETS = "hbackup.clearTestBuckets"; 
    
    private List<ObjToDelete> toDelete = new ArrayList<ObjToDelete>();

    private static S3Service sourceService;
    private static S3Service sinkService;
    private static String sourceBucket;
    private static String sinkBucket;
    
    @BeforeClass
    public static void setup() throws Exception {
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

    @Before
    public void clearBuckets() throws Exception {
        // If the user set a system property to allow clearing of the source and sink
        // buckets during testing, delete them.
        
        if(Boolean.valueOf(System.getProperty(PROP_CLEARBUCKETS, "false"))) {
            S3Object[] srcListing = sourceService.listObjects(sourceBucket);
            for(S3Object obj: srcListing) {
                sourceService.deleteObject(sourceBucket, obj.getKey());
            }
            
            S3Object[] sinkListing = sinkService.listObjects(sinkBucket);
            for(S3Object obj: sinkListing) {
                sinkService.deleteObject(sinkBucket, obj.getKey());
            }
        }

        Assert.assertEquals("Source bucket wasn't empty", 0, sourceService.listObjects(sourceBucket).length);
        Assert.assertEquals("Sink bucket wasn't empty", 0, sinkService.listObjects(sinkBucket).length);
    }
    
    /**
     * Delete objects created by the last test case.
     */
    @After
    public void cleanup() throws Exception {
        for(ObjToDelete obj: toDelete) {
            obj.s3Service.deleteObject(obj.bucket, obj.key);
        }
        toDelete.clear();
    }
    
    @Test
    public void s3BasicTest() throws Exception {
        final String contents = "ROFL ROFL LMAO";
        final String sourceKey = "from/ponies.txt";
        final String sinkKey = "to/ponies.txt";
        sourceService.putObject(sourceBucket, new S3Object(sourceKey, contents.getBytes()));
        deleteLater(sourceService, sourceBucket, sourceKey);
        deleteLater(sinkService, sinkBucket, sinkKey);
        runBackup("s3://"+sourceBucket+"/from", "s3://"+sinkBucket+"/to");
        verifyS3Obj(sinkService, sinkBucket, sinkKey, contents.getBytes());
    }

    @Test
    public void s3MultipleFilesTest() throws Exception {
        final String contents1 = "databaaaaase";
        final String contents2 = "heroes in a half shell; turtle power";
        final String sourceKey1 = "from/file1.txt";
        final String sourceKey2 = "from/file2.txt";
        final String sinkKey1 = "to/file1.txt";
        final String sinkKey2 = "to/file2.txt";
        
        // Set up files in source to be backed up
        deleteLater(sourceService, sourceBucket, sourceKey1);
        deleteLater(sourceService, sourceBucket, sourceKey2);
        deleteLater(sinkService, sinkBucket, sinkKey1);
        deleteLater(sinkService, sinkBucket, sinkKey2);
        sourceService.putObject(sourceBucket, new S3Object(sourceKey1, contents1.getBytes()));
        sourceService.putObject(sourceBucket, new S3Object(sourceKey2, contents2.getBytes()));
        
        runBackup("s3://"+sourceBucket+"/from", "s3://"+sinkBucket+"/to");
        
        verifyS3Obj(sinkService, sinkBucket, sinkKey1, contents1.getBytes());
        verifyS3Obj(sinkService, sinkBucket, sinkKey2, contents2.getBytes());
    }

    /**
     * If a file exists in the destination but has a different mtime, it should get overwritten.
     */
    @Test
    public void mtimeTest() throws Exception {
        final String initialContents = "databaaaaase";
        final String modifiedContents = "heroes in a half shell; turtle power";
        final String sourceKey = "from/file1.txt";
        final String sinkKey = "to/file1.txt";
        
        // Set up file in source to be backed up
        deleteLater(sourceService, sourceBucket, sourceKey);
        deleteLater(sinkService, sinkBucket, sinkKey);
        sourceService.putObject(sourceBucket, new S3Object(sourceKey, initialContents.getBytes()));
        verifyS3Obj(sourceService, sourceBucket, sourceKey, initialContents.getBytes());
        
        // Backup from source to dest and verify that it worked
        runBackup("s3://"+sourceBucket+"/from", "s3://"+sinkBucket+"/to");
        verifyS3Obj(sinkService, sinkBucket, sinkKey, initialContents.getBytes());
        
        // Get the metadata for the uploaded object and make sure the source mtime metadata was set
        long sourceMtime = sourceService.getObject(sourceBucket, sourceKey).getLastModifiedDate().getTime();
        Map<String,Object> metadata = sinkService.getObjectDetails(sinkBucket, sinkKey).getMetadataMap();
        long sinkMtimeSource = Long.valueOf((String)(metadata.get(Constant.S3_SOURCE_MTIME)));
        Assert.assertEquals(sourceMtime, sinkMtimeSource);
        
        // Modify the source file and run another backup. The destination should pick up the change.
        sourceService.putObject(sourceBucket, new S3Object(sourceKey, modifiedContents.getBytes()));
        verifyS3Obj(sourceService, sourceBucket, sourceKey, modifiedContents.getBytes());
        runBackup("s3://"+sourceBucket+"/from", "s3://"+sinkBucket+"/to");
        verifyS3Obj(sinkService, sinkBucket, sinkKey, modifiedContents.getBytes());
    }
    
    @Test
    public void multipartTest() throws Exception {
        Assume.assumeTrue(Boolean.valueOf(System.getProperty("hbackup.expensiveTests")));
        
        // Generate six megs of random bytes to upload to S3. We need six megs because the smallest
        // allowed part size is 5 megs.
        Random rng = new Random(0);
        byte[] sixMegBuf = new byte[6 * 1024 * 1024];
        rng.nextBytes(sixMegBuf);
        assert sixMegBuf.length >= MultipartUtils.MIN_PART_SIZE;
        
        String filename = "mptest.txt";
        String sourceDir = "from";
        String sinkDir = "to";
        
        String sourceKey = sourceDir + "/" + filename;
        String sinkKey = sinkDir + "/" + filename;
        
        deleteLater(sourceService, sourceBucket, sourceKey);
        deleteLater(sinkService, sinkBucket, sinkKey);

        sourceService.putObject(sourceBucket, new S3Object(sourceKey, sixMegBuf));
        S3Tests.verifyS3Obj(sourceService, sourceBucket, sourceKey, sixMegBuf);
        
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
                new org.apache.hadoop.conf.Configuration(),
                true);
        new HBackup(conf).runWithCheckedExceptions();
        S3Tests.verifyS3Obj(sinkService, sinkBucket, sinkKey, sixMegBuf);
        
        // Get the metadata for the uploaded object and make sure the source mtime metadata was set
        long sourceMtime = sourceService.getObject(sourceBucket, sourceKey).getLastModifiedDate().getTime();
        long sinkMtimeSource = Long.valueOf((String)sinkService.getObject(sinkBucket, sinkKey).getMetadata(Constant.S3_SOURCE_MTIME));
        Assert.assertEquals(sourceMtime, sinkMtimeSource);
    }
    
    @Test
    public void multipartHdfsToS3Test() throws Exception {
        // Generate six megs of random bytes to upload to S3. We need six megs because the smallest
        // allowed part size is 5 megs.
        
        Random rng = new Random(0);
        byte[] sixMegBuf = new byte[6 * 1024 * 1024];
        rng.nextBytes(sixMegBuf);
        assert sixMegBuf.length >= MultipartUtils.MIN_PART_SIZE;

        MiniDFSCluster hdfsCluster = null;
        try {
            hdfsCluster = new MiniDFSCluster(new Configuration(), 1, true, null);
            FileSystem fs = hdfsCluster.getFileSystem();
            OutputStream os = fs.create(new Path("/sixmegfile.txt"));
            os.write(sixMegBuf);
            os.close();
            
            final String prefix = "multipart_hdfs_to_s3";
            final String filename = "sixmegfile.txt";
            String key = prefix + "/" + filename;
            deleteLater(sinkService, sinkBucket, key);
            
            SystemConfiguration sysProps = new SystemConfiguration();
            HBackupConfig conf = new HBackupConfig(
                    "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/",
                    "s3://" + sinkBucket + "/" + prefix,
                    2, 
                    true, 
                    null, 
                    null, 
                    sysProps.getString(HBackupConfig.CONF_SINKS3ACCESSKEY), 
                    sysProps.getString(HBackupConfig.CONF_SINKS3SECRET),
                    MultipartUtils.MIN_PART_SIZE, // Smallest part size (5MB) will cause multipart upload of 6MB file 
                    MultipartUtils.MIN_PART_SIZE, // Use multipart upload if the object is at least this many bytes
                    new org.apache.hadoop.conf.Configuration(),
                    true);
            new HBackup(conf).runWithCheckedExceptions();
            S3Tests.verifyS3Obj(sinkService, sinkBucket, key, sixMegBuf);
            
        } finally {
            if(hdfsCluster != null) {
                hdfsCluster.shutdown();
            }
        }
        
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
        TestUtil.assertStreamEquals(contents, is);
    }
    
    public static void runBackup(String from, String to) throws Exception {
        HBackupConfig conf = HBackupConfig.forTests(from, to);
        new HBackup(conf).runWithCheckedExceptions();
    }
    
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
}
