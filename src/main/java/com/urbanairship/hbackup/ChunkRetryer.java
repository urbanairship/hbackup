package com.urbanairship.hbackup;

import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Runs a RetryableChunk, retrying if an IOException occurs, then writes its checksum.
 */
public class ChunkRetryer implements Runnable {
    private static Logger log = LogManager.getLogger(ChunkRetryer.class);
    
    private final FileTransferState file;
    private final RetryableChunk retryableRunnable;
    private final ChecksumService checksumService;

    private int numRetries;

    /**
     * @param fileExceptions must be a thread safe set, e.g. one from Collections.synchronizedSet().
     * This is a set of HBFiles for which a transfer exception occurred.
     */
    public ChunkRetryer(FileTransferState file, RetryableChunk retryableRunnable, 
            ChecksumService checksumService, int numRetries, Stats stats) {
        this.file = file;
        this.retryableRunnable = retryableRunnable;
        this.numRetries = numRetries;
        this.checksumService = checksumService;
    }
    
    @Override
    public void run() {
        String relativePath = file.getSourceFile().getRelativePath(); 
        if(file.getState() == FileTransferState.State.ERROR) {
            log.info("Skipping chunk because some other chunk failed in its file: " + relativePath);
            file.chunkSkipped();
            return;
        }

        StreamingXor checksum;
        // Keep retrying until the chunk doesn't throw an exception (success) or we run out of retries.
        int tryNum = 0;
        while(true) {
            try {
                checksum = retryableRunnable.run();
                if(file.chunkSuccess(checksum)) {
                    // This was the last chunk. Commit all chunks.
                    retryableRunnable.commitAllChunks();
                    file.fileCommitted();
                    if(checksumService != null) {
                        saveChecksum();
                    }
                }
                break;
            } catch (IOException e) {
                if(tryNum >= numRetries) {
                    log.error("Exhausted retries for chunk belonging to file " + 
                            relativePath, e);
                    file.chunkError(e);
                    return;
                } else {
                    log.warn("Chunk transfer error, will retry", e);
                }
                tryNum++;
            }
        }
    }
    
    private void saveChecksum() {
        // Use the same number for retries for saving the checksum as for saving the the files,
        // because whatever.
        int checksumRetriesRemaining = numRetries;
        String relativePath = file.getSourceFile().getRelativePath();
        do {
            try {
                checksumService.storeChecksum(file.getSourceFile().getRelativePath(),
                        file.getCombinedChecksum());
                file.checksumSuccess();
                return;
            } catch (IOException e) {
                log.error("Failed writing checksum for file " + relativePath, e);
            }
        } while(checksumRetriesRemaining-- > 0);
        log.error("All checksum write attempts failed for file " + relativePath);
        file.checksumFailed();
    }
}
