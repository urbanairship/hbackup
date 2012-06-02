/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup.datasinks;

import com.google.common.collect.ImmutableList;
import com.urbanairship.hbackup.*;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In memory data sink used for testing. The class is a singleton that will hold {@link SourceFile}s in memory.
 */
public class InMemoryDataSink extends Sink {

    private static final Logger log = LogManager.getLogger(InMemoryDataSink.class);

    private static InMemoryDataSink instance = new InMemoryDataSink();
    private final Collection<SourceFile> inMemoryFileSystem;


    private InMemoryDataSink() {
        this.inMemoryFileSystem = new CopyOnWriteArrayList<SourceFile>();
    }

    public static InMemoryDataSink getInstance() {
        return instance;
    }

    public Collection<SourceFile> getInMemoryFileSystem() {
        return inMemoryFileSystem;
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

           ByteArrayOutputStream byteOutputStream = null;
           @Override
           public StreamingXor run() throws IOException {
               InputStream is = file.getFullInputStream();
               XorInputStream xis = new XorInputStream(is, 0);

               byteOutputStream = new ByteArrayOutputStream((int) file.getLength());
               IOUtils.copyLarge(xis, byteOutputStream);
               is.close();
               byteOutputStream.close();
               inMemoryFileSystem.add(file);
               return xis.getStreamingXor();
           }

           @Override
           public void commitAllChunks() throws IOException {
               log.debug("In Memory no-op for commit");
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

        for (SourceFile sourceFile : inMemoryFileSystem) {
            if (sourceFile.getRelativePath().equals(relativePath)) {
                return sourceFile;
            }
        }
        return null;
    }
}
