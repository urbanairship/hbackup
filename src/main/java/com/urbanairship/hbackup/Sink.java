/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup;

import com.urbanairship.hbackup.datasinks.HdfsSink;
import com.urbanairship.hbackup.datasinks.InMemoryDataSink;
import com.urbanairship.hbackup.datasinks.Jets3tSink;
import com.urbanairship.hbackup.datasinks.LocalDiskDataSink;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * A "sink" is a place to store data. Each type of Sink is an implementation of this abstract class. New
 * Sinks can easily be added by providing a Sink implementation and modifying forUri() below.
 */
public abstract class Sink {
    public static Sink forUri(URI uri, HBackupConfig conf, Stats stats) 
            throws IOException, URISyntaxException {
        String scheme = uri.getScheme();

        ChecksumService checksumService = null;
        if(conf.checksumUri != null) {
            checksumService = ChecksumService.forUri(new URI(conf.checksumUri), conf);
        }

        if(scheme.equals("s3")) {
            return new Jets3tSink(uri, conf, stats, checksumService);
        } else if (scheme.equals("hdfs") || scheme.equals("maprfs")) {
            return new HdfsSink(uri, conf, stats, checksumService);
        } else if (scheme.equals("memory")) {
            return InMemoryDataSink.getInstance();
        }else if (scheme.equals("file")) {
            return new LocalDiskDataSink(uri, conf, stats, checksumService);
        }
        else {
            throw new IllegalArgumentException("Unknown protocol \"" + scheme + "\" in  URI " + uri);
        }
    }
    
    /**
     * Check whether the target has sourceFile and if it's up to date.
     * @return false if 
     *  - the target does not have sourceFile
     *  - the target was copied from a sourceFile with a different modification time. Each sink
     *    should remember the mtime of the source when it receives a file.
     *  - the target's version of sourceFile has a different length
     *  Otherwise true (the target is up to date). 
     */
    public abstract boolean existsAndUpToDate(SourceFile file) throws IOException;
    
    public abstract List<RetryableChunk> getChunks(SourceFile file);

    /**
     * @return the file mtime as UTC epoch millis if the file exists, or null if it doesn't exist.
     */
    public abstract Long getMTime(String relativePath) throws IOException;
}
