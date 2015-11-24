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
import java.util.List;

/**
 * Local Filesystem data sink.
 */
public class LocalDiskDataSink extends Sink {

    private String baseName;
    private File file;


    public LocalDiskDataSink(URI uri, HBackupConfig conf, Stats stats, ChecksumService checksumService) {
        this.file = new File(uri);
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
                   long size = file.getLength();
                   if(size == 0){
                       ensureFileExists(file);
                   }
                   is = file.getFullInputStream();
                   XorInputStream xis = new XorInputStream(is, 0);
                   outputStream = FileUtils.openOutputStream(getLocalFile(file));
                   IOUtils.copyLarge(xis, outputStream);
                   is.close();
                   System.out.println(String.format("Copied: %s, size %d", file.getRelativePath(), size));
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
               getLocalFile(file).setLastModified(file.getMTime());
           }
       });
    }

    private void ensureFileExists(SourceFile file) throws IOException {
        File localFile = getLocalFile(file);
        if(!localFile.getParentFile().exists()){
            localFile.getParentFile().mkdirs();
        }
        localFile.createNewFile();
    }

    private File getLocalFile(SourceFile file) {
        return new File(URI.create("file:/" + baseName + file.getRelativePath()));
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
        return new LocalSourceFile(new File(file, relativePath), file.getAbsolutePath());
    }
}
