package com.urbanairship.hbackup;

public class Constant {
    // When storing an object in S3, we store the modification time of its source file in its
    // metadata, under this metadata key. This lets us detect whether the source file was modified
    // since it was written.
    public static String S3_SOURCE_MTIME = "hbackup.sourcemtime";
    
}
