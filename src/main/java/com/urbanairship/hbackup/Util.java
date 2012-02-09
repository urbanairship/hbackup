package com.urbanairship.hbackup;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Util {
    public static final int COPY_BUF_SIZE = 100000;
    
    private Util() {
        // no instances allowed
    }
    
    public static void copyStream(InputStream is, OutputStream os) throws IOException {
        byte[] copyBuf = new byte[COPY_BUF_SIZE];
  
        // Code is similar to:
        //  http://java.sun.com/docs/books/performance/1st_edition/html/JPIOPerformance.fm.html
        while(true) {
            int bytesRead = is.read(copyBuf);
            if(bytesRead == -1) {
                break;
            }
            os.write(copyBuf, 0, bytesRead);
        }
    }
}
