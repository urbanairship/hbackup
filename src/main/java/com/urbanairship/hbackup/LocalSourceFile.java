package com.urbanairship.hbackup;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
* Date: 11/12/12
* Time: 12:16 PM
*/
public class LocalSourceFile implements SourceFile{
    private File file;
    private String basePath;

    public LocalSourceFile(File file){
        this.file = file;
        this.basePath = null;
    }

    public LocalSourceFile(File file, String basePath){
        this.file = file;
        this.basePath = basePath;
    }

    @Override
    public InputStream getFullInputStream() throws IOException {
        return new FileInputStream(file);
    }

    @Override
    public InputStream getPartialInputStream(long offset, long len) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        inputStream.skip(offset);
        return inputStream;
    }

    @Override
    public String getRelativePath() {
        if(basePath == null){
            return file.getAbsolutePath();
        }
        return file.getAbsolutePath().replace(basePath + "/", "");
    }

    @Override
    public long getMTime() throws IOException {
        return file.lastModified();
    }

    @Override
    public long getLength() {
        return file.length();
    }

    public static void fill(File parent, List<SourceFile> children) {
        if(parent.isDirectory()){
            File[] kids = parent.listFiles();
            for (File kid : kids) {
                if(kid.isDirectory()){
                    fill(kid, children);
                    continue;
                }
                children.add(new LocalSourceFile(kid));
            }
        }
    }

}
