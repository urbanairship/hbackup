/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup.checksumverify;

import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.urbanairship.hbackup.ChecksumService;
import com.urbanairship.hbackup.SourceFile;
import com.urbanairship.hbackup.StreamingXor;

/**
 * File checksums are calculated in 1 or more chunks. Each chunk is handled by a different thread in the
 * work queue. We need some coordination between the chunks so that the last chunk to finish
 * checksumming will combine all the part checksums together and report whether the checksum matched
 * with the expected checksum.
 */
public class ChecksumStateMachine {
    private static Logger log = LogManager.getLogger(ChecksumStateMachine.class);
    
    private final SourceFile sourceFile;
    private final int expectedChecksumFetchRetries;
    private final StreamingXor checksumSoFar = new StreamingXor();
    private final ChecksumService checksumService;
    private final ChecksumStats stats;

    private String expectedChecksum = null;
    private State state = State.PRE_START;
    private int chunksInProgress;
    
    public enum State {PRE_START, IN_PROGRESS, FINISHED, ERROR};
    
    public ChecksumStateMachine(SourceFile sourceFile, int numChunks, int expectedChecksumFetchRetries,
            ChecksumService checksumService, ChecksumStats stats) {
        this.sourceFile = sourceFile;
        this.chunksInProgress = numChunks;
        this.expectedChecksumFetchRetries = expectedChecksumFetchRetries;
        this.checksumService = checksumService;
        this.stats = stats;
    }
    
    /**
     * @return whether the caller should proceed with checksum calculation. If false, something has
     * already gone wrong and its chunk should be skipped.
     */
    public synchronized boolean chunkStarting() {
        switch(state) {
        case PRE_START:
//            expectedChecksum = loadExpectedChecksumWithRetries();
            // Load the the expected, pre-existing checksum from the remote source, with retries
            int retriesRemaining = expectedChecksumFetchRetries;
            String relativePath = sourceFile.getRelativePath();
            do {
                try {
                    expectedChecksum = checksumService.getChecksum(relativePath);
                    if(expectedChecksum == null) {
                        stats.missingChecksums.incrementAndGet();
                        state = State.ERROR;
                        return false;
                    } else {
                        state = State.IN_PROGRESS;
                        return true;
                    }
                } catch (IOException e) {
                    log.error("IOException fetching expected checksum for " + relativePath + ", " +
                            retriesRemaining + " retries remaining", e);
                }
            } while(retriesRemaining-- > 0);
            log.error("Retries exhausted reading expected checksum for " + relativePath);
            state = State.ERROR;
            stats.unreadableChecksums.incrementAndGet();
            stats.chunksSkipped.incrementAndGet();
            return false;
        case ERROR:
            log.debug("chunkStarting() returning false because " + sourceFile.getRelativePath() + 
                    " has already had an error");
            stats.chunksSkipped.incrementAndGet();
            return false;
        case IN_PROGRESS:
            return true;
        default:
            throw new AssertionError("Invalid state " + state);
        }
    }
    
    public synchronized void chunkFinished(StreamingXor checksum) {
        switch(state) {
        case ERROR:
            return;
        case IN_PROGRESS:
            this.checksumSoFar.update(checksum);
            chunksInProgress--;
            if(chunksInProgress == 0) {
                state = State.FINISHED;
                if(expectedChecksum.equals(checksumSoFar.getXorHex())) {
                    stats.matched.incrementAndGet();
                } else {
                    stats.mismatched.incrementAndGet();
                }
            }
            return;
        default:
            throw new AssertionError("Invalid state " + state);
        }
    }
    
    public synchronized void chunkReadError(IOException e) {
        if(e != null) {
            stats.workerExceptions.add(e);
        }
        
        switch(state) {
        case IN_PROGRESS:
            stats.unreadableChunks.incrementAndGet();
            stats.unreadableFiles.incrementAndGet();
            state = State.ERROR;
            return;
        case ERROR:
            stats.unreadableChunks.incrementAndGet();
            return;
        default:
            throw new AssertionError("Invalid state: " + state);    
        }
    }
    
    public void chunkReadError() {
        chunkReadError(null);
    }
}
