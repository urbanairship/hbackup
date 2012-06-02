/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup;

public class Util {
    /**
     * For our purposes, we define a "canonical S3 base name" as the part of the S3 object that would
     * correspond to the directory name. For example "Docs/pics/ohai.jpg" has a basename of "Docs/pics/".
     * For another example, the basename of "myfile.txt" is "". 
     * 
     * Base names don't begin with '/', and must end with '/' (except that '/' by itself is invalid).
     */
    public static String canonicalizeBaseName(String path) {
        if(!path.endsWith("/")) {
            path = path + "/";
        }
        if(path.startsWith("/")) {
            path = path.substring(1);            
        }
        return path;
    }
}
