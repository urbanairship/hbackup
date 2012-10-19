/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup.datasinks;

import com.google.common.collect.ImmutableList;
import com.urbanairship.hbackup.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Local File system data sink.
 */
public class LocalDiskDataSink extends Sink {

    private final Collection<SourceFile> fileSystem;
    private String baseName;


    public LocalDiskDataSink(URI uri, HBackupConfig conf, Stats stats, ChecksumService checksumService) {
        this.fileSystem = new CopyOnWriteArrayList<SourceFile>();
        this.baseName = Util.canonicalizeBaseName(uri.getPath());
    }

    @Override
    public boolean existsAndUpToDate(SourceFile file) throws IOException {
        SourceFile sourceFile = getSourceFile(file.getRelativePath());
        if (sourceFile != null) {
            return getMTime(sourceFile.getRelativePath()).equals(file.getMTime());
        }
        return false;
    }

    @Override
    public List<RetryableChunk> getChunks(final SourceFile file) {
       return ImmutableList.<RetryableChunk>of(new RetryableChunk() {

           OutputStream outputStream = null;

           @Override
           public StreamingXor run() throws IOException {
               InputStream is = null;
               try {
                   is = file.getFullInputStream();
                   XorInputStream xis = new XorInputStream(is, 0);
                   outputStream = FileUtils.openOutputStream(new File(baseName + file.getRelativePath()));
                   IOUtils.copyLarge(xis, outputStream);
                   is.close();
                   fileSystem.add(file);
                   return xis.getStreamingXor();
               } finally {
                   if (outputStream != null) {
                       outputStream.close();
                   }
                   if (is != null){
                       is.close();
                   }
               }
           }

           @Override
           public void commitAllChunks() throws IOException {
           }
       });
    }

    @Override
    public Long getMTime(String relativePath) throws IOException {
        SourceFile sourceFile = getSourceFile(relativePath);
        if (sourceFile == null) {
            return null;
        }

        return sourceFile.getMTime();
    }
    
    private SourceFile getSourceFile(String relativePath) {

        for (SourceFile sourceFile : fileSystem) {
            if (sourceFile.getRelativePath().equals(relativePath)) {
                return sourceFile;
            }
        }
        return null;
    }
}
