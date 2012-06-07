/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup.datasources;

import com.urbanairship.hbackup.Source;
import com.urbanairship.hbackup.SourceFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In memory data source used for testing. The class is a singleton that will hold {@link SourceFile}s in memory.
 */
public class InMemoryDataSource extends Source {

    private static InMemoryDataSource instance = new InMemoryDataSource();
    private final Collection<SourceFile> inMemoryFileSystem;

    private InMemoryDataSource() {
        this.inMemoryFileSystem = new CopyOnWriteArrayList<SourceFile>();
    }

    public Collection<SourceFile> getInMemoryFileSystem() {
        return inMemoryFileSystem;
    }

    @Override
    public List<SourceFile> getFiles(boolean recursive) throws IOException {
        return new ArrayList<SourceFile>(inMemoryFileSystem);
    }

    public static InMemoryDataSource getInstance() {
        return instance;
    }
}
