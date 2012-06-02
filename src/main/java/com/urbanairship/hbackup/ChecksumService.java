/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup;

import java.io.IOException;
import java.net.URI;

public abstract class ChecksumService {
    public static ChecksumService forUri(URI uri, HBackupConfig conf) throws IOException {
        String scheme = uri.getScheme();
        
        if (scheme.equals("s3")) {
            return new Jets3tChecksumImpl(uri, conf);
        } else {
            throw new IllegalArgumentException("Invalid URI scheme \"" + scheme + "\" for checksum storage");
        }
    }
    
    public abstract void storeChecksum(String relativePath, String hexChecksum) throws IOException;
    
    public abstract String getChecksum(String relativePath) throws IOException;
}
