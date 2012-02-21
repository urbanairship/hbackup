package com.urbanairship.hbackup;

import java.io.IOException;

/**
 * An interface for runnable-like objects that can be retried if an IOException occurs.
 */
public interface RetryableChunk {
    public StreamingXor run() throws IOException;
    
    public void commitAllChunks() throws IOException;
}
