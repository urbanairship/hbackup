package com.urbanairship.hbackup.datasources;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.common.io.LimitInputStream;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.Source;
import com.urbanairship.hbackup.SourceFile;

public class HdfsSource extends Source {
    private static final Logger log = LogManager.getLogger(HdfsSource.class);
    private final DistributedFileSystem dfs;
    private final URI baseUri;
    
    public HdfsSource(URI sourceUri, HBackupConfig conf) 
            throws IOException, URISyntaxException {
        this.baseUri = sourceUri;
        org.apache.hadoop.conf.Configuration hadoopConf = conf.hdfsSourceConf;
        FileSystem fs = FileSystem.get(baseUri, hadoopConf);
        if(!(fs instanceof DistributedFileSystem)) {
            throw new RuntimeException("Hadoop FileSystem instance for URI was not an HDFS DistributedFileSystem");
        }
        dfs = (DistributedFileSystem)fs;
    }

    @Override
    public List<SourceFile> getFiles(boolean recursive) throws IOException {
        List<SourceFile> hbFiles = new ArrayList<SourceFile>();
        addFiles(hbFiles, new Path(baseUri), recursive, "");
        return hbFiles;
    }
    
    private void addFiles(List<SourceFile> files, Path path, boolean recursive, String relativeTo) throws IOException {
        FileStatus[] listing = dfs.listStatus(path);
        
        if(listing == null) {
            return;
        }
        for(FileStatus stat: listing) {
            if(stat.isDir()) {
               if(recursive) {
                   addFiles(files, stat.getPath(), recursive, relativeTo + stat.getPath().getName() + "/");
               }
            } else { // stat isn't a directory, so it's a file
                String filename = stat.getPath().toUri().getPath(); // Looks like /dir/dir/filename
                long fileLength = stat.getLen();
                
                LocatedBlocks fileBlocks = dfs.getClient().namenode.getBlockLocations(filename, 0, fileLength);
                if(fileBlocks.isUnderConstruction()) { 
                    log.debug("Skipping file under construction: " + filename);
                } else {
                    files.add(new HdfsFile(stat, dfs, relativeTo + stat.getPath().getName()));
                }
            }
        }
    }
    
    /**
     * An implementation of SourceFile that knows how to read from HDFS. 
     */
    private class HdfsFile implements SourceFile {
        private final FileStatus stat;
        private final DistributedFileSystem dfs;
        private final String relativePath;
        
        public HdfsFile(FileStatus stat, DistributedFileSystem dfs, String relativePath) {
            this.stat = stat;
            this.dfs = dfs;
            this.relativePath = relativePath;
            assert !relativePath.startsWith("/");
        }
        
        @Override
        public InputStream getFullInputStream() throws IOException {
            return dfs.open(stat.getPath());
        }
        
        @Override
        public InputStream getPartialInputStream(long offset, long len) throws IOException {
            FSDataInputStream is = dfs.open(stat.getPath());
            is.seek(offset);
            return new LimitInputStream(is, len);
        }
        
        /**
         * @return The filename used by both the source and the target. This is relative 
         * to the base directory of the source. For example, if the source file was 
         * "hdfs://localhost:7080/base/mypics/pony.png", and the base URI was 
         * "hdfs://localhost:7080.base", the relativePath would be "/mypics/pony.png"
         */
        @Override
        public String getRelativePath() {
            return relativePath;
        }
        
        @Override
        public long getMTime() {
            return stat.getModificationTime();
        }
        
        @Override
        public long getLength() {
            return stat.getLen();
        }
    }
}
