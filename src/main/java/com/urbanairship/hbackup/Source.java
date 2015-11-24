/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup;

import com.urbanairship.hbackup.datasources.HdfsSource;
import com.urbanairship.hbackup.datasources.InMemoryDataSource;
import com.urbanairship.hbackup.datasources.Jets3tSource;
import com.urbanairship.hbackup.datasources.LocalDiskDataSource;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * A "source" is a place from which files are retrieved. Each type of Source is an implementation of this 
 * abstract class. New Sources can easily be added by providing a Source implementation and modifying forUri() 
 * below.
 */
public abstract class Source {
    public static Source forUri(URI uri, HBackupConfig conf) throws IOException, URISyntaxException {
        String scheme = uri.getScheme();
        
        if (scheme.equals("s3")) {
            return new Jets3tSource(uri, conf);
        } else if (scheme.equals("hdfs") || scheme.equals("maprfs")) {
            return new HdfsSource(uri, conf);
        } else if (scheme.equals("memory")) {
           return InMemoryDataSource.getInstance();
        } else if (scheme.equals("file")) {
            return new LocalDiskDataSource(uri, conf);
        } else {
            throw new IllegalArgumentException("Unknown URI scheme \"" + scheme + "\" in  URI " + uri);
        }
    }

    /**
     * Get the list of files present in the source. These are candidates for copying.
     */
    public abstract List<SourceFile> getFiles(boolean recursive) throws IOException;
}
