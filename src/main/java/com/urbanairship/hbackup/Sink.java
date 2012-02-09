package com.urbanairship.hbackup;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.urbanairship.hbackup.datasinks.HdfsSink;
import com.urbanairship.hbackup.datasinks.Jets3tSink;

/**
 * A "sink" is a place to store data. Each type of Sink is an implementation of this abstract class. New
 * Sinks can easily be added by providing a Sink implementation and modifying forUri() below.
 */
public abstract class Sink {
    public static Sink forUri(URI uri, HBackupConfig conf) throws IOException, URISyntaxException {
        String scheme = uri.getScheme();
        
        if(scheme.equals("s3")) {
            return new Jets3tSink(uri, conf);
        } else if (scheme.equals("hdfs")) {
            return new HdfsSink(uri, conf);
        } else {
            throw new IllegalArgumentException("Unknown protocol \"" + scheme + "\" in  URI " + uri);
        }
    }
    
    /**
     * Check whether the target has sourceFile and if it's up to date.
     * @return false if 
     *  - the target does not have sourceFile
     *  - the target's version of sourceFile has an older modification time
     *  - the target's version of sourceFile has a different length
     *  Otherwise true (the target is up to date). 
     */
    public abstract boolean existsAndUpToDate(HBFile file) throws IOException;
    
    public abstract void write(HBFile file) throws IOException;
}
