/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup;

import com.urbanairship.hbackup.datasinks.InMemoryDataSink;
import com.urbanairship.hbackup.datasources.InMemoryDataSource;
import com.urbanairship.hbackup.service.tasks.HBackupScheduled;
import com.urbanairship.hbackup.service.tasks.StaleCheckScheduled;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.*;
/**
 */
public class BackupServiceTest {
    
    private static final Logger log = LogManager.getLogger(BackupServiceTest.class);

    //Get the source and dest memory filesystems
    InMemoryDataSource inMemoryDataSource = InMemoryDataSource.getInstance();
    InMemoryDataSink inMemoryDataSink = InMemoryDataSink.getInstance();

    @Test
    public void backupTest() throws Exception {
        
        final String path = "/testStuff.dat";
        final int lengthOfFile = new Random().nextInt(4096);
        final long timeStampOfFile = new DateTime().getMillis();

        // Put a test file in the source
        addTestFile(inMemoryDataSource.getInMemoryFileSystem(), path, lengthOfFile, timeStampOfFile);

        // Configure and start/stop the service. It should run exactly once.
        HBackupConfig config = HBackupConfig.forTests("memory:/"+path, "memory:/",1,-1);
        HBackupScheduled hBackupScheduled = new HBackupScheduled(config);
        hBackupScheduled.startAndWait();
        hBackupScheduled.stopAndWait();

        // Assert everything went as planned.
        Collection<SourceFile> inMemoryFileSystem = inMemoryDataSink.getInMemoryFileSystem();
        for (SourceFile sourceFile : inMemoryFileSystem) {
            assertEquals(path, sourceFile.getRelativePath());
            assertEquals(lengthOfFile, sourceFile.getLength());
            assertEquals(timeStampOfFile, sourceFile.getMTime());
        }

        Stats lastRunStats = hBackupScheduled.getLastRunStats();
        assertEquals(1, lastRunStats.numFilesSucceeded.get());
        assertEquals(0, lastRunStats.numFilesFailed.get());
        assertEquals(1, lastRunStats.numChunksSucceeded.get());

        assertEquals(1,hBackupScheduled.getTimerMetric().count());
        assertTrue(hBackupScheduled.getTimerMetric().mean() > 0.0);
        clearMemoryFS();
    }

    @Test
    public void staleCheckTestWithStaleFiles() throws Exception {

        final String path = "/testStuff.dat";
        final int lengthOfFile = new Random().nextInt(4096);
        final long timeStampOfFile = new DateTime().getMillis();

        //Get the source and dest memory filesystems
        InMemoryDataSource inMemoryDataSource = InMemoryDataSource.getInstance();
        InMemoryDataSink inMemoryDataSink = InMemoryDataSink.getInstance();

        // Configure and start/stop the service. It should run exactly once.
        HBackupConfig config = HBackupConfig.forTests("memory:/"+path, "memory:/",0,1);


        // Put a test file in the source
        addTestFile(inMemoryDataSource.getInMemoryFileSystem(), path, lengthOfFile, timeStampOfFile);
        addTestFile(inMemoryDataSink.getInMemoryFileSystem(), path, lengthOfFile, timeStampOfFile - (config.stalenessMillis + 1000));

        StaleCheckScheduled staleCheckScheduled = new StaleCheckScheduled(config);
        staleCheckScheduled.startAndWait();
        staleCheckScheduled.stopAndWait();

        StaleCheckStats lastRunStats = staleCheckScheduled.getLastRunStats();

        assertEquals(0, lastRunStats.nonStaleFiles.get());
        assertEquals(1, lastRunStats.staleFiles.get());
        assertEquals(0, lastRunStats.failedFiles.get());

        assertNotSame(1, staleCheckScheduled.getTimerMetric().count());
        clearMemoryFS();

    }

    @Test
    public void staleCheckTestWithNoStaleFiles() throws Exception {

        final String path = "/testStuff.dat";
        final int lengthOfFile = new Random().nextInt(4096);
        final long timeStampOfFile = new DateTime().getMillis();

        //Get the source and dest memory filesystems
        InMemoryDataSource inMemoryDataSource = InMemoryDataSource.getInstance();
        InMemoryDataSink inMemoryDataSink = InMemoryDataSink.getInstance();

        // Configure and start/stop the service. It should run exactly once.
        HBackupConfig config = HBackupConfig.forTests("memory:/"+path, "memory:/",0,1);


        // Put a test file in the source
        addTestFile(inMemoryDataSource.getInMemoryFileSystem(), path, lengthOfFile, timeStampOfFile);
        addTestFile(inMemoryDataSink.getInMemoryFileSystem(), path, lengthOfFile, timeStampOfFile + (config.stalenessMillis + 1000));

        StaleCheckScheduled staleCheckScheduled = new StaleCheckScheduled(config);
        staleCheckScheduled.startAndWait();
        Thread.sleep(1000);
        staleCheckScheduled.stopAndWait();

        StaleCheckStats lastRunStats = staleCheckScheduled.getLastRunStats();

        assertEquals(1, lastRunStats.nonStaleFiles.get());
        assertEquals(0, lastRunStats.staleFiles.get());
        assertEquals(0, lastRunStats.failedFiles.get());

        assertNotSame(0, staleCheckScheduled.getTimerMetric().count());
        clearMemoryFS();
    }

    private void addTestFile(Collection<SourceFile> memoryFileSystem, final String path, final int length, final long time) {
        final ByteBuffer allocate = ByteBuffer.allocate(length);
        memoryFileSystem.add(new SourceFile() {
            @Override
            public InputStream getFullInputStream() throws IOException {
                return new ByteArrayInputStream(allocate.array());
            }

            @Override
            public InputStream getPartialInputStream(long offset, long len) throws IOException {
                return new ByteArrayInputStream(allocate.array(),(int) offset,(int) len);
            }

            @Override
            public String getRelativePath() {
                return path;
            }

            @Override
            public long getMTime() throws IOException {
                return time;
            }

            @Override
            public long getLength() {
                return length;
            }
        });
    }

    @After
    public void clearMemoryFS() {
        inMemoryDataSink.getInMemoryFileSystem().clear();
        inMemoryDataSource.getInMemoryFileSystem().clear();
    }
}
