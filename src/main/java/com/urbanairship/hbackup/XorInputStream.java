package com.urbanairship.hbackup;

import java.io.IOException;
import java.io.InputStream;

/**
 * This input stream wraps another input stream and keeps track of a running XOR of its bytes.
 */
public class XorInputStream extends InputStream {
    private final InputStream wrapStream;
    
    long inputOffset = 0;
    StreamingXor streamingXor = new StreamingXor();
    
    public XorInputStream(InputStream wrapStream, long startingOffset) {
        this.wrapStream = wrapStream;
        this.inputOffset = startingOffset;
    }

      @Override
    public int available() throws IOException {
        return wrapStream.available();
    }

    @Override
    public void close() throws IOException {
        wrapStream.close();
    }

    @Override
    public synchronized void mark(int arg0) {
        throw new RuntimeException("Mark not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public int read() throws IOException {
        int b = wrapStream.read();
        if(b >= 0) {
            streamingXor.updateXor((byte)b, inputOffset);
            inputOffset++;
        }
        return b;
    }

    @Override
    public int read(byte[] bytes, int offset, int len) throws IOException {
        int bytesRead = wrapStream.read(bytes, offset, len);
        for(int i=0; i<bytesRead; i++) {
            streamingXor.updateXor(bytes[offset+i], inputOffset);
            inputOffset++;
        }
        return bytesRead;
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        int bytesRead = wrapStream.read(bytes);
        for(int i=0; i<bytesRead; i++) {
            streamingXor.updateXor(bytes[i], inputOffset);
            inputOffset++;
        }
        return bytesRead;
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new RuntimeException("Mark not supported");
    }

    @Override
    public long skip(long nBytes) throws IOException {
        throw new RuntimeException("Skip not supported");
    }
    
    public StreamingXor getStreamingXor() {
        return streamingXor;
    }
}
