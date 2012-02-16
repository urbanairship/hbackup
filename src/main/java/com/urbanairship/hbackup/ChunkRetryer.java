package com.urbanairship.hbackup;

import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Runs a RetryableChunk, retrying if an IOException occurs.
 */
public class ChunkRetryer implements Runnable {
    private static Logger log = LogManager.getLogger(ChunkRetryer.class);
    
    private final FileTransferState file;
    private final RetryableChunk retryableRunnable;

    private int numRetries;

    /**
     * @param fileExceptions must be a thread safe set, e.g. one from Collections.synchronizedSet().
     * This is a set of HBFiles for which a transfer exception occurred.
     */
    public ChunkRetryer(FileTransferState file, RetryableChunk retryableRunnable, int numRetries, 
            Stats stats) {
        this.file = file;
        this.retryableRunnable = retryableRunnable;
        this.numRetries = numRetries;
    }
    
    @Override
    public void run() {
        while(true) {
            try {
                if(file.getState() == FileTransferState.State.ERROR) {
                    log.info("Skipping chunk because some other chunk failed in its file: " +
                            file.getSourceFile().getRelativePath());
                    file.chunkSkipped();
                } else {
                    retryableRunnable.run();
                    if(file.chunkSuccess()) {
                        // This was the last chunk. Commit all chunks.
                        retryableRunnable.commitAllChunks();
                        file.fileCommitted();
                    }
                }
                return;
            } catch (IOException e) {
                if(numRetries-- <= 0) {
                    log.error("Exhausted retries for chunk", e);
                    file.chunkError();
                    return;
                }
            }       
        }
    }
}
