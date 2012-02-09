package com.urbanairship.hbackup;

import java.io.IOException;
import java.io.InputStream;

public abstract class HBFile {
    public abstract InputStream getInputStream() throws IOException;
    
    /**
     * @return The filename used by both the source and the target. This is relative 
     * to the base directory of the source. For example, if the source file was 
     * "hdfs://localhost:7080/base/mypics/pony.png", and the base URI was 
     * "hdfs://localhost:7080.base", the canonicalPath would be "mypics/pony.png"
     */
    public abstract String getCanonicalPath();
    
    public abstract long getMTime();
    
    public abstract long getLength();
}
