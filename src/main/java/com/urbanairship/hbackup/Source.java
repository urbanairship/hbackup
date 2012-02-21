package com.urbanairship.hbackup;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import com.urbanairship.hbackup.datasources.HdfsSource;
import com.urbanairship.hbackup.datasources.Jets3tSource;

/**
 * A "source" is a place from which files are retrieved. Each type of Source is an implementation of this 
 * abstract class. New Sources can easily be added by providing a Source implementation and modifying forUri() 
 * below.
 */
public abstract class Source {
    public static Source forUri(URI uri, HBackupConfig conf, Stats stats) throws IOException, URISyntaxException {
        String scheme = uri.getScheme();
        
        if (scheme.equals("s3")) {
            return new Jets3tSource(uri, conf, stats);
        } else if (scheme.equals("hdfs")) {
            return new HdfsSource(uri, conf, stats);
        } else {
            throw new IllegalArgumentException("Unknown URI scheme \"" + scheme + "\" in  URI " + uri);
        }
    }

    /**
     * Get the list of files present in the source. These are candidates for copying.
     */
    public abstract List<SourceFile> getFiles(boolean recursive) throws IOException;
}
