package com.urbanairship.hbackup.datasinks;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.urbanairship.hbackup.HBFile;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.Sink;
import com.urbanairship.hbackup.Util;

public class HdfsSink extends Sink {
    private static final Logger log = LogManager.getLogger(HdfsSink.class);
    private final URI baseUri;
    private final DistributedFileSystem dfs;
    private final HBackupConfig conf;
    
    public HdfsSink(URI uri, HBackupConfig conf) throws IOException, URISyntaxException {
        this.baseUri = uri;
        this.conf = conf;
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        FileSystem fs = FileSystem.get(baseUri, hadoopConf);
        if(!(fs instanceof DistributedFileSystem)) {
            throw new RuntimeException("Hadoop FileSystem instance for URI was not an HDFS DistributedFileSystem");
        }
        dfs = (DistributedFileSystem)fs;
    }
    
    @Override
    public boolean existsAndUpToDate(HBFile sourceFile) throws IOException {
        Path path = new Path(baseUri).suffix(sourceFile.getCanonicalPath());
        try {
            FileStatus targetStat = dfs.getFileStatus(path);
            if(sourceFile.getMTime() > targetStat.getModificationTime()) {
                return false;
            } else if (sourceFile.getLength() != targetStat.getLen()) {
                return false;
            } else {
                return true;
            }
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    @Override
    public void write(HBFile file) throws IOException {
        String canonicalPath = file.getCanonicalPath();
        Path destPath = new Path(baseUri).suffix(canonicalPath);
        InputStream is = file.getInputStream();
        FSDataOutputStream os = dfs.create(destPath);
        Util.copyStream(is, os);
        is.close();
        os.close();
        log.debug("Done transferring file: " + canonicalPath);
    }
}
