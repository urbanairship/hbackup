package com.urbanairship.hbackup;

import java.io.File;
import java.io.OutputStream;

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

public class HdfsTest {
    private static MiniDFSCluster srcCluster;
    private static MiniDFSCluster sinkCluster;
    private static FileSystem srcFs;
    private static FileSystem sinkFs;
    
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
        
        srcCluster = new MiniDFSCluster(0, cluster1Conf, 1, true, false, true, null, 
                null, null, null);
        sinkCluster = new MiniDFSCluster(0, cluster2Conf, 1, true, false, true, null, 
                null, null, null);

        srcFs = srcCluster.getFileSystem();
        sinkFs = sinkCluster.getFileSystem();
    }
    
    /**
     * Remove all files in the source and sink filesystems between tests to get to a pristine state.
     */
    @After
    public void cleanFs() throws Exception {
        for(FileSystem fs: new FileSystem[] {srcFs, sinkFs}) {
            for(FileStatus stat: fs.listStatus(new Path("/"))) {
                fs.delete(stat.getPath(), true);
            }
        }
    }
    
    @AfterClass
    public static void shutdownMiniDfsClusters() {
        if(srcCluster != null) {
            TestUtil.shutdownMiniDfs(srcCluster);
        }
        if(sinkCluster != null) {
            TestUtil.shutdownMiniDfs(sinkCluster);
        }
    }
    
    @Test
    public void testHdfsToHdfs() throws Exception {
        final String FILE_CONTENTS = "Unicorns are better than ponies";
        
        srcFs.mkdirs(new Path("/copysrc"));
        writeFile(srcFs, "/copysrc/myfile.txt", FILE_CONTENTS);
        
        HBackup hBackup = new HBackup(HBackupConfig.forTests(getSourceUrl("/copysrc"), 
                getSinkUrl("/copydest"), null, srcFs.getConf(),
                sinkFs.getConf(), null, null));
        hBackup.runWithCheckedExceptions();
        
        Assert.assertTrue(sinkFs.exists(new Path("/copydest/myfile.txt")));
        TestUtil.verifyHdfsContents(sinkFs, "/copydest/myfile.txt", FILE_CONTENTS);
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
        writeFile(srcFs, sourceName, initialContents);
        TestUtil.verifyHdfsContents(srcFs, sourceName, initialContents);
        
        // Backup from source to dest and verify that it worked
        HBackupConfig conf = HBackupConfig.forTests(getSourceUrl("/from"), 
                getSinkUrl("/to"), null, srcFs.getConf(), 
                sinkFs.getConf(), null, null);
        new HBackup(conf).runWithCheckedExceptions();
        TestUtil.verifyHdfsContents(sinkFs, sinkName, initialContents);
        
        // Verify that the sink file has the same mtime as the source file
        long sourceMtime = srcFs.getFileStatus(new Path(sourceName)).getModificationTime();
        long sinkMtime = sinkFs.getFileStatus(new Path(sinkName)).getModificationTime();
        Assert.assertEquals(sourceMtime, sinkMtime);
        
        // Modify the source file and run another backup. The destination should pick up the change.
        writeFile(srcFs, sourceName, modifiedContents);
        new HBackup(conf).runWithCheckedExceptions();
        TestUtil.verifyHdfsContents(sinkFs, sinkName, modifiedContents);
    }
    
    /**
     * Test regex file matching by backup up a folder containing two files, one that matches the regex
     * and one that doesn't.
     */
    @Test
    public void regexTest() throws Exception {
        writeFile(srcFs, "/from/i_do_match.txt", "Taco");
        writeFile(srcFs, "/from/i_dont_match.txt", "Burrito");
        
//        HBackupConfig conf = HBackupConfig.forTests(getSourceUrl("/from"), getSinkUrl("/to"),
//                null, srcFs.getConf(), sinkFs.getConf(), null, null);
        HBackupConfig conf = new HBackupConfig(
                getSourceUrl("/from"), 
                getSinkUrl("/to"),
                1,
                true,
                null,
                null,
                null,
                null,
                MultipartUtils.MIN_PART_SIZE,
                MultipartUtils.MIN_PART_SIZE, 
                srcFs.getConf(),
                sinkFs.getConf(),
                false, 
                ".*do_match.*",
                null,
                0,
                null,
                null);
        HBackup hbackup = new HBackup(conf);
        hbackup.runWithCheckedExceptions();
        Assert.assertEquals(1, hbackup.getStats().numFilesSucceeded.get());
        TestUtil.verifyHdfsContents(sinkFs, "/to/i_do_match.txt", "Taco");
    }
    
    /**
     * Make sure that empty files are backed up.
     */
    @Test
    public void emptyFileTest() throws Exception {
        writeFile(srcFs, "/from/empty.txt", "");
        HBackupConfig config = HBackupConfig.forTests(getSourceUrl("/from"), 
                getSinkUrl("/to"), null, srcFs.getConf(), sinkFs.getConf(), null, null);
        new HBackup(config).runWithCheckedExceptions();
        Assert.assertTrue(sinkFs.exists(new Path("/to/empty.txt")));
    }
    
    private static void writeFile(FileSystem fs, String path, String contents) throws Exception {
        OutputStream os = fs.create(new Path(path), true);
        os.write(contents.getBytes());
        os.close();
    }
    
    private static String getSourceUrl(String dirName) {
        if(dirName.startsWith("/")) {
            dirName = dirName.substring(1);
        }
        return "hdfs://localhost:" + srcCluster.getNameNodePort() + "/" + dirName;
    }

    private static String getSinkUrl(String dirName) {
        if(dirName.startsWith("/")) {
            dirName = dirName.substring(1);
        }
        return "hdfs://localhost:" + sinkCluster.getNameNodePort() + "/" + dirName;
    }
}
