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
import com.urbanairship.hbackup.HBFile;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.Sink;
import com.urbanairship.hbackup.Stats;

public class HdfsSink extends Sink {
    private static final Logger log = LogManager.getLogger(HdfsSink.class);
//    private final URI baseUri;
    private String baseUri;
    private final DistributedFileSystem dfs;
    private final Stats stats;
    private final HBackupConfig conf;
    
    public HdfsSink(URI uri, HBackupConfig conf, Stats stats) throws IOException, URISyntaxException {
        this.stats = stats;
        if(uri.toString().endsWith("/")) {
            this.baseUri = uri.toString();
        } else {
            this.baseUri = uri.toString() + "/";
        }
        this.conf = conf;
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        FileSystem fs = FileSystem.get(uri, hadoopConf);
        if(!(fs instanceof DistributedFileSystem)) {
            throw new RuntimeException("Hadoop FileSystem instance for URI was not an HDFS DistributedFileSystem");
        }
        dfs = (DistributedFileSystem)fs;
    }
    
    @Override
    public boolean existsAndUpToDate(HBFile sourceFile) throws IOException {
        Path path = new Path(baseUri + sourceFile.getRelativePath());
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
    public List<Runnable> getChunks(final HBFile sourceFile) {
        return ImmutableList.<Runnable>of(new Runnable() {
            @Override
            public void run() {
                InputStream is = null;
                FSDataOutputStream os = null;
                try {
                    String relativePath = sourceFile.getRelativePath();
                    assert !relativePath.startsWith("/");
                    Path destPath = new Path(baseUri + relativePath);
                    is = sourceFile.getFullInputStream();
                    os = dfs.create(destPath);
                    IOUtils.copyLarge(is, os);
                    is.close();
                    os.close();
                    
                    // Set the atime and mtime of the sink file equal to the mtime of the source file.
                    dfs.setTimes(destPath, sourceFile.getMTime(), sourceFile.getMTime());
                    
                    log.debug("Done transferring file: " + relativePath);
                    stats.numFilesSucceeded.incrementAndGet();
                    stats.numChunksSucceeded.incrementAndGet();
                } catch (IOException e) {
                    log.error(e);
                    stats.transferExceptions.add(e);
                    stats.numChunksFailed.incrementAndGet();
                    stats.numFilesFailed.incrementAndGet();
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
        });
    }
}
