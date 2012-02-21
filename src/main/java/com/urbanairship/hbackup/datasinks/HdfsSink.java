package com.urbanairship.hbackup.datasinks;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.urbanairship.hbackup.ChecksumService;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.RetryableChunk;
import com.urbanairship.hbackup.Sink;
import com.urbanairship.hbackup.SourceFile;
import com.urbanairship.hbackup.Stats;
import com.urbanairship.hbackup.StreamingXor;
import com.urbanairship.hbackup.XorInputStream;

public class HdfsSink extends Sink {
    private static final Logger log = LogManager.getLogger(HdfsSink.class);
    private final String baseName;
    private final DistributedFileSystem dfs;
    private final HBackupConfig conf;
    
    public HdfsSink(URI uri, HBackupConfig conf, Stats stats, ChecksumService checksumService) throws IOException, URISyntaxException {
        String tempBaseName = uri.getPath();
        if(!tempBaseName.startsWith("/")) {
            tempBaseName = "/" + tempBaseName;
        }
        if(!tempBaseName.endsWith("/")) {
            tempBaseName = tempBaseName + "/";
        }
        this.baseName = tempBaseName;
        this.conf = conf;
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        FileSystem fs = FileSystem.get(uri, hadoopConf);
        if(!(fs instanceof DistributedFileSystem)) {
            throw new RuntimeException("Hadoop FileSystem instance for URI was not an HDFS DistributedFileSystem");
        }
        dfs = (DistributedFileSystem)fs;
    }
    
    @Override
    public boolean existsAndUpToDate(SourceFile sourceFile) throws IOException {
        Path path = new Path(baseName + sourceFile.getRelativePath());
        try {
            FileStatus targetStat = dfs.getFileStatus(path);
            if (sourceFile.getLength() != targetStat.getLen()) {
                log.debug("Different length in source and sink, will re-upload: " + sourceFile.getRelativePath());
                return false;
            }
            long sourceMtime = sourceFile.getMTime();
            long sinkMtime = targetStat.getModificationTime();
            if(!conf.mtimeCheck) {
                log.debug("Same lengths and mtime checking disabled. Won't re-upload " + sourceFile.getRelativePath());
                return true;
            }
            if(sourceMtime != sinkMtime) {
                log.debug("Different mtime source and sink, " + sourceMtime + " vs " + sinkMtime + 
                        ".  Will re-upload " + sourceFile.getRelativePath());
                return false;
            } else {
                log.debug("Lengths and mtimes matched. Won't re-upload " + sourceFile.getRelativePath());
                return true;
            }
        } catch (FileNotFoundException e) {
            log.debug("Sink file " + path + " didn't exist for source file " + sourceFile.getRelativePath() +
                    ". Will re-upload.");
            return false;
        }
    }

    /**
     * HDFS files can only have a single writer at a time. Therefore we do a transfer to HDFS
     * as a single chunk, which might be large.
     */
    @Override
    public List<RetryableChunk> getChunks(final SourceFile sourceFile) {
        return ImmutableList.<RetryableChunk>of(new RetryableChunk() {
            @Override
            public StreamingXor run() throws IOException {
                InputStream is = null;
                FSDataOutputStream os = null;
                
                try {
                    String relativePath = sourceFile.getRelativePath();
                    assert !relativePath.startsWith("/");
                    Path destPath = new Path(baseName + relativePath);
                    is = sourceFile.getFullInputStream();
                    XorInputStream xis = new XorInputStream(is, 0);
                    os = dfs.create(destPath);
                    IOUtils.copyLarge(xis, os);
                    is.close();
                    os.close();
                    
                    // Set the atime and mtime of the sink file equal to the mtime of the source file.
                    dfs.setTimes(destPath, sourceFile.getMTime(), sourceFile.getMTime());

                    log.debug("Done transferring file to HDFS: " + relativePath);
                    
                    return xis.getStreamingXor();
                } finally {
                    if(is != null) {
                        try {
                            is.close();
                        } catch (IOException e) { }
                    }
                    if(os != null) {
                        try {
                            os.close();
                        } catch (IOException e) { }
                    }
                }
            }

            @Override
            public void commitAllChunks() throws IOException {
                log.debug("Commit noop for HDFS, nothing to do to commit to HDFS");
            }
        });
    }
}
