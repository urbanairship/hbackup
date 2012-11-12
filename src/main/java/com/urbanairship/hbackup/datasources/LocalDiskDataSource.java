/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup.datasources;

import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.LocalSourceFile;
import com.urbanairship.hbackup.Source;
import com.urbanairship.hbackup.SourceFile;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * In memory data source used for testing. The class is a singleton that will hold {@link com.urbanairship.hbackup.SourceFile}s in memory.
 */
public class LocalDiskDataSource extends Source {


    private List<SourceFile> allChildNodes;
    private List<SourceFile> directDescendents;
    private HBackupConfig conf;
    private final File file;

    public LocalDiskDataSource(URI uri, HBackupConfig conf) {
        this.conf = conf;
        this.file = new File(uri);
        this.directDescendents = new ArrayList<SourceFile>();
        this.allChildNodes = new ArrayList<SourceFile>();
        if(file.isDirectory()){
            File[] files = file.listFiles();
            for (File f : files) {
                if(f.isDirectory()){
                    continue;
                }
                directDescendents.add(new LocalSourceFile(f));
            }
            LocalSourceFile.fill(file, allChildNodes);
        }
    }

    @Override
    public List<SourceFile> getFiles(boolean recursive) throws IOException {
        return recursive ? new ArrayList<SourceFile>(allChildNodes): new ArrayList<SourceFile>(directDescendents);
    }

}
