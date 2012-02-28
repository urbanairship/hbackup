package com.urbanairship.hbackup.checksumverify;

import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.urbanairship.hbackup.SourceFile;
import com.urbanairship.hbackup.StreamingXor;
import com.urbanairship.hbackup.XorInputStream;

public class ChunkChecksummer implements Runnable {
    private static final Logger log = LogManager.getLogger(ChunkChecksummer.class);
    
    private final SourceFile sourceFile;
    private final long startOffset;
    private final long len;
    private final int numRetries;
    private final ChecksumStateMachine checksumStateMachine;
    
    public ChunkChecksummer(SourceFile sourceFile, long startOffset, long len, int numRetries, 
            ChecksumStateMachine checksumStateMachine) {
        this.sourceFile = sourceFile;
        this.startOffset = startOffset;
        this.len = len;
        this.numRetries = numRetries;
        this.checksumStateMachine = checksumStateMachine;
    }
    
    @Override
    public void run() {
        InputStream is = null;
        int retriesRemaining = numRetries;
        String relativePath = sourceFile.getRelativePath();
        
        boolean shouldProcessChunk = checksumStateMachine.chunkStarting();
        if(!shouldProcessChunk) {
            return;
        }
        
        IOException lastException = null;
        do {
            try {
                is = sourceFile.getPartialInputStream(startOffset, len);
                XorInputStream xis = new XorInputStream(is, startOffset);
                byte[] ignoreBuf = new byte[16384];
                
                while(xis.read(ignoreBuf) != -1) {  } // Read bytes until input stream exhausted
                
                StreamingXor streamingXor = xis.getStreamingXor();
                checksumStateMachine.chunkFinished(streamingXor);
                return;
            } catch (IOException e) {
                log.error("IOException when verifying checksum for chunk of " + relativePath + 
                        ", " + retriesRemaining + " retries remaining");
                lastException = e;
            } finally {
                if(is != null) {
                    try {
                        is.close();
                    } catch (IOException e) { }
                    is = null;
                }
            }
        } while (retriesRemaining-- > 0);
        log.error("All retries exhausted trying to checksum " + relativePath);
        checksumStateMachine.chunkReadError(lastException);
    }
}
