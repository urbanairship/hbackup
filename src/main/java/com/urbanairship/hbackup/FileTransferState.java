package com.urbanairship.hbackup;

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
    
    synchronized State getState() {
        return state;
    }
    
    public SourceFile getSourceFile() {
        return sourceFile;
    }
    
    synchronized void chunkError() {
        if(state != State.PENDING && state != State.ERROR) {
            throw new RuntimeException("Invalid state " + state);
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
    synchronized boolean chunkSuccess(StreamingXor checksum) {
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
    
    synchronized void chunkSkipped() {
        if(state != State.ERROR) {
            throw new RuntimeException("Invalid state " + state);
        }
        stats.numChunksSkipped.incrementAndGet();
        chunksOutstanding--;
    }
    
    synchronized void fileCommitted() {
        if(state != State.CHUNKS_COMPLETE) {
            throw new RuntimeException("Invalid state " + state);
        }
        state = State.COMMITTED;
        stats.numFilesSucceeded.incrementAndGet();
    }
    
    synchronized public String getCombinedChecksum() {
        return combinedChecksum.getXorHex();
    }
}
