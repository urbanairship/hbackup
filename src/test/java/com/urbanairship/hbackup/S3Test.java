package com.urbanairship.hbackup;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.configuration.SystemConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.utils.MultipartUtils;
import org.junit.After;
import org.junit.AfterClass;
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
 *  
 *  In maven, you can do: mvn -DargLine="-Dhbackup.x=y -Dhbackup.z=w" test
 */
public class S3Test {
    public static final String PROP_CLEARBUCKETS = "hbackup.clearTestBuckets"; 
    public static final String PROP_DOS3TESTS = "hbackup.doS3Tests";
    
    private List<ObjToDelete> toDelete = new ArrayList<ObjToDelete>();

    private static S3Service sourceService;
    private static S3Service sinkService;
    private static String sourceBucket;
    private static String sinkBucket;
    private static MiniDFSCluster dfsCluster = null;
    
    @BeforeClass
    public static void setup() throws Exception {
        sourceBucket = System.getProperty("hbackup.test.sourceBucket");  
        sinkBucket = System.getProperty("hbackup.test.destBucket"); 
        
        // S3 tests aren't free, so skip them unless enabled by system property
        Assume.assumeTrue(Boolean.valueOf(System.getProperty(PROP_DOS3TESTS, "false")));
        
        Assume.assumeNotNull(System.getProperty(HBackupConfig.CONF_SINKS3ACCESSKEY));
        Assume.assumeNotNull(System.getProperty(HBackupConfig.CONF_SOURCES3ACCESSKEY));
        Assume.assumeNotNull(System.getProperty(HBackupConfig.CONF_SINKS3SECRET));
        Assume.assumeNotNull(System.getProperty(HBackupConfig.CONF_SOURCES3SECRET));
        Assume.assumeNotNull(sourceBucket, sinkBucket);
        
        HBackupConfig throwawayConf = HBackupConfig.forTests("fakesource", "fakeddest");
        sourceService = new RestS3Service(throwawayConf.s3SourceCredentials);
        sinkService = new RestS3Service(throwawayConf.s3SinkCredentials);
        
        dfsCluster = new MiniDFSCluster(new Configuration(), 1, true, null);
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
        
        FileSystem fs = dfsCluster.getFileSystem();
        for(FileStatus stat: fs.listStatus(new Path("/"))) {
            fs.delete(stat.getPath(), true);
        }
    }
    
    @AfterClass
    public static void shutdownMiniDfsClusters() {
        TestUtil.shutdownMiniDfs(dfsCluster);
    }
    
    @Test
    public void tests3BasicTest() throws Exception {
        final String contents = "ROFL ROFL LMAO";
        final String sourceKey = "from/ponies.txt";
        final String sinkKey = "to/ponies.txt";
        sourceService.putObject(sourceBucket, new S3Object(sourceKey, contents.getBytes()));
        deleteLater(sourceService, sourceBucket, sourceKey);
        deleteLater(sinkService, sinkBucket, sinkKey);
        runBackup("s3://"+sourceBucket+"/from", "s3://"+sinkBucket+"/to");
        TestUtil.verifyS3Obj(sinkService, sinkBucket, sinkKey, contents.getBytes());
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
        
        TestUtil.verifyS3Obj(sinkService, sinkBucket, sinkKey1, contents1.getBytes());
        TestUtil.verifyS3Obj(sinkService, sinkBucket, sinkKey2, contents2.getBytes());
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
        TestUtil.verifyS3Obj(sourceService, sourceBucket, sourceKey, initialContents.getBytes());
        
        // Backup from source to dest and verify that it worked
        runBackup("s3://"+sourceBucket+"/from", "s3://"+sinkBucket+"/to");
        TestUtil.verifyS3Obj(sinkService, sinkBucket, sinkKey, initialContents.getBytes());
        
        // Get the metadata for the uploaded object and make sure the source mtime metadata was set
        long sourceMtime = sourceService.getObjectDetails(sourceBucket, sourceKey).getLastModifiedDate().getTime();
        Map<String,Object> metadata = sinkService.getObjectDetails(sinkBucket, sinkKey).getMetadataMap();
        long sinkMtimeSource = Long.valueOf((String)(metadata.get(Constant.S3_SOURCE_MTIME)));
        Assert.assertEquals(sourceMtime, sinkMtimeSource);
        
        // Modify the source file and run another backup. The destination should pick up the change.
        sourceService.putObject(sourceBucket, new S3Object(sourceKey, modifiedContents.getBytes()));
        TestUtil.verifyS3Obj(sourceService, sourceBucket, sourceKey, modifiedContents.getBytes());
        runBackup("s3://"+sourceBucket+"/from", "s3://"+sinkBucket+"/to");
        TestUtil.verifyS3Obj(sinkService, sinkBucket, sinkKey, modifiedContents.getBytes());
    }
    
    @Test
    public void multipartTest() throws Exception {
        byte[] sixMegBuf = getRandomBuf(6*1024*1024);

        String filename = "mptest.txt";
        String sourceDir = "from";
        String sinkDir = "to";
        
        String sourceKey = sourceDir + "/" + filename;
        String sinkKey = sinkDir + "/" + filename;
        
        deleteLater(sourceService, sourceBucket, sourceKey);
        deleteLater(sinkService, sinkBucket, sinkKey);

        sourceService.putObject(sourceBucket, new S3Object(sourceKey, sixMegBuf));
        TestUtil.verifyS3Obj(sourceService, sourceBucket, sourceKey, sixMegBuf);
        
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
                true,
                null,
                null,
                0,
                null,
                null);
        new HBackup(conf).runWithCheckedExceptions();
        TestUtil.verifyS3Obj(sinkService, sinkBucket, sinkKey, sixMegBuf);
        
        // Get the metadata for the uploaded object and make sure the source mtime metadata was set
        long sourceMtime = sourceService.getObjectDetails(sourceBucket, sourceKey).getLastModifiedDate().getTime();
        long sinkMtimeSource = Long.valueOf((String)sinkService.getObjectDetails(sinkBucket, sinkKey).getMetadata(Constant.S3_SOURCE_MTIME));
        Assert.assertEquals(sourceMtime, sinkMtimeSource);
    }
    
    @Test
    public void multipartHdfsToS3Test() throws Exception {
        byte[] sixMegBuf = getRandomBuf(6*1024*1024);
        
        FileSystem fs = dfsCluster.getFileSystem();
        OutputStream os = fs.create(new Path("/sixmegfile.txt"));
        os.write(sixMegBuf);
        os.close();
        
        final String prefix = "multipart_hdfs_to_s3";
        final String filename = "sixmegfile.txt";
        String key = prefix + "/" + filename;
        deleteLater(sinkService, sinkBucket, key);
        
        SystemConfiguration sysProps = new SystemConfiguration();
        HBackupConfig conf = new HBackupConfig(
                "hdfs://localhost:" + dfsCluster.getNameNodePort() + "/",
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
                true,
                null,
                null,
                0,
                null,
                null);
        new HBackup(conf).runWithCheckedExceptions();
        TestUtil.verifyS3Obj(sinkService, sinkBucket, key, sixMegBuf);
    }

    /**
     * Make sure files don't get re-uploaded to S3 if the source mtime doesn't change.
     * @throws Exception
     */
    @Test
    public void hdfsToS3MtimeTest() throws Exception {
        byte[] oneKBuf = getRandomBuf(1024);

        FileSystem fs = dfsCluster.getFileSystem();
        String filename = "/one_k_file.txt";
        OutputStream os = fs.create(new Path(filename));
        os.write(oneKBuf);
        os.close();
        
        deleteLater(sinkService, sinkBucket, filename);
        
        String sourceUri = "hdfs://localhost:" + dfsCluster.getNameNodePort() + filename; 
        
        SystemConfiguration sysProps = new SystemConfiguration();
        HBackupConfig conf = new HBackupConfig(
                sourceUri,
                "s3://" + sinkBucket + "/prefix",
                2,
                true,
                null,
                null,
                sysProps.getString(HBackupConfig.CONF_SINKS3ACCESSKEY), 
                sysProps.getString(HBackupConfig.CONF_SINKS3SECRET),
                MultipartUtils.MIN_PART_SIZE, // Smallest part size (5MB) will cause multipart upload of 6MB file 
                MultipartUtils.MIN_PART_SIZE, // Use multipart upload if the object is at least this many bytes
                new org.apache.hadoop.conf.Configuration(),
                true,
                null,
                null,
                0,
                null,
                null);
        
        // The first time we run a backup, the file should be copied over.
        HBackup hbackup;
        hbackup = new HBackup(conf);
        hbackup.runWithCheckedExceptions();
        Assert.assertEquals(1, hbackup.getStats().numFilesSucceeded.get());
        
        // If we re-run the backup, the file should be skipped since it's up to date.
        hbackup = new HBackup(conf);
        hbackup.runWithCheckedExceptions();
        Assert.assertEquals(0, hbackup.getStats().numFilesSucceeded.get());
        Assert.assertEquals(1, hbackup.getStats().numUpToDateFilesSkipped.get());
    }
    
    @Test
    public void emptyFileS3ToHdfs() throws Exception {
        String filename = "empty_file.txt";
        byte[] contents = new byte[] {};
        deleteLater(sourceService, sourceBucket, filename);
        sourceService.putObject(sourceBucket, new S3Object(filename, contents));
        TestUtil.verifyS3Obj(sourceService, sourceBucket, filename, contents);
        
        TestUtil.runBackup("s3://" + sourceBucket, "hdfs://localhost:" + dfsCluster.getNameNodePort());
        TestUtil.verifyHdfsContents(dfsCluster.getFileSystem(), filename, "");
    }
    
    @Test
    public void simpleChecksumTest() throws Exception {
        String filename = "abc.txt";
        String contents = "abc";
        
        deleteLater(sourceService, sourceBucket, filename);
        deleteLater(sinkService, sinkBucket, filename);
        sourceService.putObject(sourceBucket, new S3Object(filename, contents));
        TestUtil.verifyS3Obj(sourceService, sourceBucket, filename, contents.getBytes());
        
        SystemConfiguration sysProps = new SystemConfiguration();
        HBackupConfig conf = new HBackupConfig(
                "s3://" + sourceBucket,
                "s3://" + sinkBucket,
                2,
                true,
                sysProps.getString(HBackupConfig.CONF_SOURCES3ACCESSKEY), 
                sysProps.getString(HBackupConfig.CONF_SOURCES3SECRET),
                sysProps.getString(HBackupConfig.CONF_SINKS3ACCESSKEY), 
                sysProps.getString(HBackupConfig.CONF_SINKS3SECRET),
                MultipartUtils.MIN_PART_SIZE, // Smallest part size (5MB) will cause multipart upload of 6MB file 
                MultipartUtils.MIN_PART_SIZE, // Use multipart upload if the object is at least this many bytes
                new org.apache.hadoop.conf.Configuration(),
                true,
                null,
                "s3://" + sinkBucket + "/hashes",
                0,
                sysProps.getString(HBackupConfig.CONF_CHECKSUMS3ACCESSKEY),
                sysProps.getString(HBackupConfig.CONF_CHECKSUMS3SECRET));
        HBackup hBackup;
        hBackup = new HBackup(conf);
        hBackup.runWithCheckedExceptions();
        Assert.assertEquals(1, hBackup.getStats().numFilesSucceeded.get());
        
        String expectedHash = XorStreamTest.expectedXor(contents.getBytes()); 
        TestUtil.verifyS3Obj(sinkService, sinkBucket, "hashes/abc.txt", expectedHash.getBytes());
    }
    
    @Test
    public void multipartChecksumTest() throws Exception {
        String filename = "bigrandom.txt";
        byte[] contents = getRandomBuf(6 * 1024 * 1024);
        
        deleteLater(sourceService, sourceBucket, filename);
        deleteLater(sinkService, sinkBucket, filename);
        sourceService.putObject(sourceBucket, new S3Object(filename, contents));
        
        SystemConfiguration sysProps = new SystemConfiguration();
        HBackupConfig conf = new HBackupConfig(
                "s3://" + sourceBucket,
                "s3://" + sinkBucket,
                2,
                true,
                sysProps.getString(HBackupConfig.CONF_SOURCES3ACCESSKEY), 
                sysProps.getString(HBackupConfig.CONF_SOURCES3SECRET),
                sysProps.getString(HBackupConfig.CONF_SINKS3ACCESSKEY), 
                sysProps.getString(HBackupConfig.CONF_SINKS3SECRET),
                MultipartUtils.MIN_PART_SIZE, // Smallest part size (5MB) will cause multipart upload of 6MB file 
                MultipartUtils.MIN_PART_SIZE, // Use multipart upload if the object is at least this many bytes
                new org.apache.hadoop.conf.Configuration(),
                true,
                null,
                "s3://" + sinkBucket + "/hashes",
                0,
                sysProps.getString(HBackupConfig.CONF_CHECKSUMS3ACCESSKEY),
                sysProps.getString(HBackupConfig.CONF_CHECKSUMS3SECRET));
        HBackup hBackup;
        hBackup = new HBackup(conf);
        hBackup.runWithCheckedExceptions();
        Assert.assertEquals(1, hBackup.getStats().numFilesSucceeded.get());
        
        TestUtil.verifyS3Obj(sinkService, sinkBucket, "hashes/bigrandom.txt",
                XorStreamTest.expectedXor(contents).getBytes());
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
    
    static byte[] getRandomBuf(int size) {
        Random rng = new Random(0);
        byte[] buf = new byte[size];
        rng.nextBytes(buf);
        return buf;
    }
}
