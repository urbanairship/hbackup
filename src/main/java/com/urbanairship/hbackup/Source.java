package com.urbanairship.hbackup;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import com.urbanairship.hbackup.datasources.HdfsSource;

/**
 * A "source" is a place from which files are retrieved. Each type of Source is an implementation of this 
 * abstract class. New Sources can easily be added by providing a Source implementation and modifying forUri() 
 * below.
 */
public abstract class Source {
    public static Source forUri(URI uri, HBackupConfig conf) throws IOException, URISyntaxException {
        String scheme = uri.getScheme();
        
        if (scheme.equals("s3")) {
//            return new Jets3tSource(url, conf);
            throw new AssertionError("s3 source not supported yet");
        } else if (scheme.equals("hdfs")) {
            return new HdfsSource(uri, conf);
        } else {
            throw new IllegalArgumentException("Unknown URI scheme \"" + scheme + "\" in  URI " + uri);
        }
    }

    public abstract List<HBFile> getFiles(boolean recursive) throws IOException;
}
