package com.urbanairship.hbackup;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

import com.urbanairship.hbackup.HBackup;

public class IntegrationTest {
    
    @Test
    public void testHdfsToHdfs() throws Exception {
        final String FILE_CONTENTS = "Unicorns are better than ponies";
        
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
        
        MiniDFSCluster srcCluster = new MiniDFSCluster(0, cluster1Conf, 1, true, false, true, null, 
                null, null, null);
        MiniDFSCluster destCluster = new MiniDFSCluster(0, cluster2Conf, 1, true, false, true, null, 
                null, null, null);
        
        FileSystem srcFs = srcCluster.getFileSystem();
        srcFs.mkdirs(new Path("/copysrc"));
        FSDataOutputStream os = srcFs.create(new Path("/copysrc/myfile.txt"));
        os.write(FILE_CONTENTS.getBytes());
        os.close();
        
        FileSystem destFs = destCluster.getFileSystem();
        destFs.mkdirs(new Path("/copydest"));
        
        
        String sourceUrl = "hdfs://localhost:" + srcCluster.getNameNodePort() + "/copysrc";
        String destUrl  =  "hdfs://localhost:" + destCluster.getNameNodePort() + "/copydest";
        HBackup hBackup = new HBackup(new HBackupConfig(sourceUrl, destUrl));
        hBackup.runWithCheckedExceptions();
        
        destFs.exists(new Path("/copydest/myfile.txt"));
    }
}
