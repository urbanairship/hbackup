package com.urbanairship.hbackup;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StalenessCheckerTest extends S3SetupAndTeardownTest {
    // TODO file has special mtime metadata
    // TODO file doesn't have special mtime metadata
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        S3SetupAndTeardownTest.beforeClass();
    }
    
    @AfterClass
    public static void afterClass() {
        S3SetupAndTeardownTest.afterClass();
    }
    
    @Before
    public void before() throws Exception {
        super.beforeEach();
    }
    
    @After
    public void after() throws Exception {
        super.afterEach();
    }
    
    @Test
    public void basic() throws Exception {
        // Back up a file into S3. It will have the special "source mtime" S3 metadata.
        String filename = "/source/the_file";
        TestUtil.writeHdfsFile(dfsCluster.getFileSystem(), filename, "abcdefg");
        deleteLater(sinkService, sinkBucket, filename);
        HBackupConfig hbackupConfig = HBackupConfig.forTests(getHdfsSourceUrl("/source"), 
                "s3://" + sinkBucket + "/files", dfsClusterConfig);
        HBackup hBackup = new HBackup(hbackupConfig);
        hBackup.runWithCheckedExceptions();
        Assert.assertEquals(1, hBackup.getStats().numFilesSucceeded.get());
        
        StalenessCheck stalenessCheck = new StalenessCheck(hbackupConfig);
        Assert.assertEquals(0, stalenessCheck.runWithCheckedExceptions());
        Assert.assertEquals(0, stalenessCheck.getStats().staleFiles.get());
        Assert.assertEquals(1, stalenessCheck.getStats().nonStaleFiles.get());
        Assert.assertEquals(0, stalenessCheck.getStats().failedFiles.get());
    }
    
    @Test
    public void oneStaleFile() throws Exception {
        String filename = "/source/the_file";

        // Create a file in HDFS with an old timestamp and back it up.
        TestUtil.writeHdfsFile(dfsCluster.getFileSystem(), filename, "abcdefg");
        long longTimeAgo = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1000);
        dfsCluster.getFileSystem().setTimes(new Path(filename), longTimeAgo, longTimeAgo);
        deleteLater(sinkService, sinkBucket, filename);
        HBackupConfig hbackupConfig = HBackupConfig.forTests(getHdfsSourceUrl("/source"), 
                "s3://" + sinkBucket + "/files", dfsClusterConfig);
        HBackup hBackup = new HBackup(hbackupConfig);
        hBackup.runWithCheckedExceptions();
        Assert.assertEquals(1, hBackup.getStats().numFilesSucceeded.get());
        
        // Set the timestamp on the HDFS file to now. This will cause the staleness checker to see
        // the backed-up version as being very stale.
        long now = System.currentTimeMillis();
        dfsCluster.getFileSystem().setTimes(new Path(filename), now, now);
        
        // Do the staleness check. There should be only one file, which should be stale.
        StalenessCheck stalenessCheck = new StalenessCheck(hbackupConfig);
        Assert.assertFalse(stalenessCheck.runWithCheckedExceptions() == 0);
        Assert.assertEquals(1, stalenessCheck.getStats().staleFiles.get());
        Assert.assertEquals(0, stalenessCheck.getStats().nonStaleFiles.get());
        Assert.assertEquals(0, stalenessCheck.getStats().failedFiles.get());
    }
    
    @Test
    public void oneStaleOneNot() throws Exception {
        // Create a situation where one of the two files is stale, make sure the test returns
        // nonzero and the counters show one of each stale/nonstale.
        
        String staleFileName = "/source/stalefile";
        String freshFileName = "/source/freshfile";
        
        TestUtil.writeHdfsFile(dfsCluster.getFileSystem(), staleFileName, "abcdefg");
        TestUtil.writeHdfsFile(dfsCluster.getFileSystem(), freshFileName, "hijklmnop");
        long longTimeAgo = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1000);
        dfsCluster.getFileSystem().setTimes(new Path(staleFileName), longTimeAgo, longTimeAgo);
        deleteLater(sinkService, sinkBucket, staleFileName);
        deleteLater(sinkService, sinkBucket, freshFileName);
        HBackupConfig hbackupConfig = HBackupConfig.forTests(getHdfsSourceUrl("/source"), 
                "s3://" + sinkBucket + "/files", dfsClusterConfig);
        HBackup hBackup = new HBackup(hbackupConfig);
        hBackup.runWithCheckedExceptions();
        Assert.assertEquals(2, hBackup.getStats().numFilesSucceeded.get());
        
        // Set the timestamp on the HDFS file to now. This will cause the staleness checker to see
        // the backed-up version as being very stale.
        long now = System.currentTimeMillis();
        dfsCluster.getFileSystem().setTimes(new Path(staleFileName), now, now);
        
        StalenessCheck stalenessCheck = new StalenessCheck(hbackupConfig);
        Assert.assertFalse(stalenessCheck.runWithCheckedExceptions() == 0);
        Assert.assertEquals(1, stalenessCheck.getStats().staleFiles.get());
        Assert.assertEquals(1, stalenessCheck.getStats().nonStaleFiles.get());
        Assert.assertEquals(0, stalenessCheck.getStats().failedFiles.get());
    }
    
    @Test
    public void nonExistentSink() throws Exception {
        String fakeSinkBucket = "i_dont_exist_in_s3_should_throw_exception_U32932BD922bds91Ba";
        
        TestUtil.writeHdfsFile(dfsCluster.getFileSystem(), "/source/testfile", "abcdefg");
        
        HBackupConfig hbackupConfig = HBackupConfig.forTests(getHdfsSourceUrl("/source"), 
                "s3://" + fakeSinkBucket + "/files", dfsClusterConfig);
        
        StalenessCheck stalenessCheck = new StalenessCheck(hbackupConfig);
        Assert.assertFalse(stalenessCheck.runWithCheckedExceptions() == 0);
    }
    
    @Test
    public void emptySource() throws Exception {
        // When no files are found in the sink, make sure the staleness check returns nonzero. 
        HBackupConfig hbackupConfig = HBackupConfig.forTests(getHdfsSourceUrl("/source"), 
                "s3://" + sinkBucket + "/files", dfsClusterConfig);
        StalenessCheck stalenessCheck = new StalenessCheck(hbackupConfig);
        Assert.assertFalse(stalenessCheck.runWithCheckedExceptions() == 0);
    }
    
    private static String getHdfsSourceUrl(String dirName) {
        if(dirName.startsWith("/")) {
            dirName = dirName.substring(1);
        }
        return "hdfs://localhost:" + dfsCluster.getNameNodePort() + "/" + dirName;
    }
}
