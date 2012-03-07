package com.urbanairship.hbackup;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.junit.Assert;
import org.junit.Assume;

/**
 * We have multiple test classes that need to do S3 init and teardown. Those classes can inherit this 
 * one and call super.setup()
 */
public class S3SetupAndTeardownTest {
    public static final String PROP_CLEARBUCKETS = "hbackup.clearTestBuckets"; 
    public static final String PROP_DOS3TESTS = "hbackup.doS3Tests";
    
    protected List<ObjToDelete> toDelete = new ArrayList<ObjToDelete>();

    protected static S3Service sourceService;
    protected static S3Service sinkService;
    protected static String sourceBucket;
    protected static String sinkBucket;
    protected static MiniDFSCluster dfsCluster = null;
    protected static Configuration dfsClusterConfig;
    
    protected static void beforeClass() throws Exception {
        sourceBucket = System.getProperty("hbackup.test.sourceBucket");  
        sinkBucket = System.getProperty("hbackup.test.destBucket"); 
        
        // S3 tests aren't free, so skip them unless enabled by system property
        Assume.assumeTrue(Boolean.valueOf(System.getProperty(PROP_DOS3TESTS, "false")));
        
        Assume.assumeNotNull(System.getProperty(HBackupConfig.CONF_SINKS3ACCESSKEY));
        Assume.assumeNotNull(System.getProperty(HBackupConfig.CONF_SOURCES3ACCESSKEY));
        Assume.assumeNotNull(System.getProperty(HBackupConfig.CONF_SINKS3SECRET));
        Assume.assumeNotNull(System.getProperty(HBackupConfig.CONF_SOURCES3SECRET));
        Assume.assumeNotNull(sourceBucket, sinkBucket);
        
        HBackupConfig throwawayConf = HBackupConfig.forTests("fakesource", "fakedest", null);
        sourceService = new RestS3Service(throwawayConf.s3SourceCredentials);
        sinkService = new RestS3Service(throwawayConf.s3SinkCredentials);
        
        dfsCluster = new MiniDFSCluster(new Configuration(), 1, true, null);
        dfsClusterConfig = dfsCluster.getFileSystem().getConf();
    }
    
    /** If the user set a system property to allow clearing of the source and sink
      * buckets during testing, delete their contents.
      */
    protected void beforeEach() throws Exception {
        
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
    protected void afterEach() throws Exception {
        for(ObjToDelete obj: toDelete) {
            obj.s3Service.deleteObject(obj.bucket, obj.key);
        }
        toDelete.clear();
        
        FileSystem fs = dfsCluster.getFileSystem();
        for(FileStatus stat: fs.listStatus(new Path("/"))) {
            fs.delete(stat.getPath(), true);
        }
    }
    
    protected static void afterClass() {
        if(dfsCluster != null) {
            TestUtil.shutdownMiniDfs(dfsCluster);
        }
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
    
    protected void deleteLater(S3Service service, String bucket, String key) {
        toDelete.add(new ObjToDelete(service, bucket, key));
    }
}
