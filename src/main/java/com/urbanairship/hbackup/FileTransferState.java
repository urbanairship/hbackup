/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup;

import java.io.IOException;

public class FileTransferState {
    private final SourceFile sourceFile;
    private final Stats stats;
    private final StreamingXor combinedChecksum = new StreamingXor(); 
    
    public enum State {PENDING, ERROR, CHUNKS_COMPLETE, COMMITTED};
    private State state = State.PENDING;
    private int chunksOutstanding;
    
    public FileTransferState(SourceFile file, int numChunks, Stats stats) {
        this.sourceFile = file;
        this.stats = stats;
        this.chunksOutstanding = numChunks;
    }
    
    synchronized public State getState() {
        return state;
    }
    
    public SourceFile getSourceFile() {
        return sourceFile;
    }
    
    synchronized public void chunkError() {
        chunkError(null);
    }
    
    synchronized public void chunkError(IOException e) {
        if(state != State.PENDING && state != State.ERROR) {
            throw new RuntimeException("Invalid state " + state);
        }
        
        if(state == State.PENDING && e != null) {
            // This is the first exception for this file. Save it so we can print a diagnostic
            // message later.
            stats.fileFailureExceptions.add(e);
        }
        stats.numChunksFailed.incrementAndGet();
        chunksOutstanding--;
        if(state != State.ERROR) { 
            state = State.ERROR;
            stats.numFilesFailed.incrementAndGet();
        } 
    }
    
    /**
     * @return whether all chunks for the file have now been sent. The caller may want to "commit" the
     * new object since all chunks have finished.
     */
    synchronized public boolean chunkSuccess(StreamingXor checksum) {
        combinedChecksum.update(checksum);
        if(state != State.PENDING && state != State.ERROR) {
            throw new RuntimeException("Invalid state " + state);
        }
        stats.numChunksSucceeded.incrementAndGet();
        chunksOutstanding--;
        if(chunksOutstanding == 0) {
            state = State.CHUNKS_COMPLETE;
            return true;
        } else {
            return false;
        }
    }
    
    synchronized public void chunkSkipped() {
        if(state != State.ERROR) {
            throw new RuntimeException("Invalid state " + state);
        }
        stats.numChunksSkipped.incrementAndGet();
        chunksOutstanding--;
    }
    
    synchronized public void fileCommitted() {
        if(state != State.CHUNKS_COMPLETE) {
            throw new RuntimeException("Invalid state " + state);
        }
        state = State.COMMITTED;
        stats.numFilesSucceeded.incrementAndGet();
    }
    
    synchronized public String getCombinedChecksum() {
        return combinedChecksum.getXorHex();
    }
    
    public void checksumSuccess() {
        stats.numChecksumsSucceeded.incrementAndGet();
    }
    
    public void checksumFailed() {
        stats.numChecksumsFailed.incrementAndGet();
    }
}
