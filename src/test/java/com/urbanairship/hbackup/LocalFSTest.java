/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.jets3t.service.utils.MultipartUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class LocalFSTest {
    private static MiniDFSCluster hdfsSrcCluster;
    private static MiniDFSCluster hdfsSinkCluster;
    private static FileSystem hdfsSrcFs;
    private static FileSystem hdfsSinkFs;
    private static String tmpDir;


    @BeforeClass
    public static void setupHdfsClusters() throws Exception {
        // We have to specify the MiniDFSClusters' directories manually, otherwise both clusters 
        // will try to use the hardcoded defaults and collide with each other.
        // This code is partly based on some of the code in MiniDFSCluster.java.
        File baseDir = new File(System.getProperty("test.build.data", "build/test/data"), "dfs/");
        Configuration cluster1Conf = new Configuration(true);
        Configuration cluster2Conf = new Configuration(true);
        cluster1Conf.set("dfs.name.dir", new File(baseDir, "cluster1_name1").getPath() + "," +
                new File(baseDir, "cluster1_name2").getPath());
        cluster2Conf.set("dfs.name.dir", new File(baseDir, "cluster2_name1").getPath() + "," +
                new File(baseDir, "cluster2_name2").getPath());
        cluster1Conf.set("fs.checkpoint.dir", new File(baseDir, "cluster1_sec1").getPath() + "," +
                new File(baseDir, "cluster1_sec2").getPath());
        cluster2Conf.set("fs.checkpoint.dir", new File(baseDir, "cluster2_sec1").getPath() + "," +
                new File(baseDir, "cluster2_sec2").getPath());
        
        hdfsSrcCluster = new MiniDFSCluster(0, cluster1Conf, 1, true, false, true, null,
                null, null, null);
        hdfsSinkCluster = new MiniDFSCluster(0, cluster2Conf, 1, true, false, true, null,
                null, null, null);

        hdfsSrcFs = hdfsSrcCluster.getFileSystem();
        hdfsSinkFs = hdfsSinkCluster.getFileSystem();
        File dir = new File("/tmp/test_" + UUID.randomUUID().toString() + "/");
        dir.mkdir();
        tmpDir = dir.getAbsolutePath();

    }
    
    /**
     * Remove all files in the source and sink filesystems between tests to get to a pristine state.
     */
    @After
    public void cleanFs() throws Exception {
        for(FileSystem fs: new FileSystem[] {hdfsSrcFs, hdfsSinkFs}) {
            for(FileStatus stat: fs.listStatus(new Path("/"))) {
                fs.delete(stat.getPath(), true);
            }
        }
    }
    
    @AfterClass
    public static void shutdownMiniDfsClusters() {
        if(hdfsSrcCluster != null) {
            TestUtil.shutdownMiniDfs(hdfsSrcCluster);
        }
        if(hdfsSinkCluster != null) {
            TestUtil.shutdownMiniDfs(hdfsSinkCluster);
        }
    }
    
    @Test
    public void testHdfsToLocal() throws Exception {
        final String FILE_CONTENTS = "Unicorns are better than ponies";
        
        hdfsSrcFs.mkdirs(new Path("/copysrc"));
        TestUtil.writeHdfsFile(hdfsSrcFs, "/copysrc/myfile.txt", FILE_CONTENTS);
        
        HBackup hBackup = new HBackup(HBackupConfig.forTests(getSourceUrl("/copysrc"),
                getLocalUrl(tmpDir + "/copydest"), null, hdfsSrcFs.getConf(),
                null, null, null));
        hBackup.runWithCheckedExceptions();

        File localFile = new File(tmpDir + "/copydest/myfile.txt");
        Assert.assertTrue(localFile.exists());
        TestUtil.verifyLocalContents(tmpDir + "/copydest/myfile.txt", FILE_CONTENTS);
    }
    
    /**
     * If a file exists in the destination but has a different mtime, it should get overwritten.
     */
    @Test
    public void mtimeTest() throws Exception {
        final String initialContents = "databaaaaase";
        final String modifiedContents = "heroes in a half shell; turtle power";
        final String sourceName = "/from/file1.txt";
        final String sinkName = "/to/file1.txt";
        
        // Set up file in source to be backed up
        TestUtil.writeHdfsFile(hdfsSrcFs, sourceName, initialContents);
        TestUtil.verifyHdfsContents(hdfsSrcFs, sourceName, initialContents);
        
        // Backup from source to dest and verify that it worked
        HBackupConfig conf = HBackupConfig.forTests(getSourceUrl("/from"),
                getLocalUrl(tmpDir + "/to"), null, hdfsSrcFs.getConf(),
                null, null, null);
        new HBackup(conf).runWithCheckedExceptions();
        TestUtil.verifyLocalContents(tmpDir + sinkName, initialContents);
        
        // Verify that the sink file has the same mtime as the source file
        long sourceMtime = hdfsSrcFs.getFileStatus(new Path(sourceName)).getModificationTime();
        long sinkMtime = new File(tmpDir + sinkName).lastModified();
        Assert.assertEquals(sourceMtime/1000, sinkMtime/1000);
        
        // Modify the source file and run another backup. The destination should pick up the change.
        TestUtil.writeHdfsFile(hdfsSrcFs, sourceName, modifiedContents);
        new HBackup(conf).runWithCheckedExceptions();
        TestUtil.verifyLocalContents(tmpDir + sinkName, modifiedContents);
    }
    
    /**
     * Test regex file matching by backup up a folder containing two files, one that matches the regex
     * and one that doesn't.
     */
    @Test
    public void regexTest() throws Exception {
        TestUtil.writeHdfsFile(hdfsSrcFs, "/from/i_do_match.txt", "Taco");
        TestUtil.writeHdfsFile(hdfsSrcFs, "/from/i_dont_match.txt", "Burrito");

        HBackupConfig conf = new HBackupConfig(
                getSourceUrl("/from"), 
                getLocalUrl(tmpDir + "/to"),
                1,
                true,
                null,
                null,
                null,
                null,
                MultipartUtils.MIN_PART_SIZE,
                MultipartUtils.MIN_PART_SIZE, 
                hdfsSrcFs.getConf(),
                null,
                false, 
                ".*do_match.*",
                null,
                0,
                null,
                null,
                null,
                null,
                0,
                0,
                0);
        HBackup hbackup = new HBackup(conf);
        hbackup.runWithCheckedExceptions();
        Assert.assertEquals(1, hbackup.getStats().numFilesSucceeded.get());
        TestUtil.verifyLocalContents(tmpDir + "/to/i_do_match.txt", "Taco");
    }
    
    /**
     * Make sure that empty files are backed up.
     */
    @Test
    public void emptyFileTest() throws Exception {
        TestUtil.writeHdfsFile(hdfsSrcFs, "/from/empty.txt", "");
        HBackupConfig config = HBackupConfig.forTests(getSourceUrl("/from"),
                getLocalUrl(tmpDir + "/to"), null, hdfsSrcFs.getConf(), hdfsSinkFs.getConf(), null, null);
        new HBackup(config).runWithCheckedExceptions();
        Assert.assertTrue(new File(tmpDir + "/to/empty.txt").exists());
    }
    
    private static String getSourceUrl(String dirName) {
        if(dirName.startsWith("/")) {
            dirName = dirName.substring(1);
        }
        return "hdfs://localhost:" + hdfsSrcCluster.getNameNodePort() + "/" + dirName;
    }

    private static String getSinkUrl(String dirName) {
        if(dirName.startsWith("/")) {
            dirName = dirName.substring(1);
        }
        return "hdfs://localhost:" + hdfsSinkCluster.getNameNodePort() + "/" + dirName;
    }

    private static String getLocalUrl(String dirName) {
        if(dirName.startsWith("/")) {
            dirName = dirName.substring(1);
        }
        return "file:/" + dirName;
    }
}
