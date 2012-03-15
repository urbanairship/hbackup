package com.urbanairship.hbackup.service;

import com.urbanairship.hbackup.Stats;
import org.joda.time.DateTime;
import org.joda.time.Seconds;

/**
 */
public class ScheduledBackupStats implements ScheduledBackupStatsMXBean {

    
    private Stats stats = new Stats();
    private DateTime lastRunDate = new DateTime();
    
    public void setStats(Stats stats) {
        this.stats = stats;
        this.lastRunDate = new DateTime();
    }


    @Override
    public int getNumUpToDateFilesSkipped() {
        return stats.numUpToDateFilesSkipped.get();
    }

    @Override
    public int getNumChunksFailed() {
        return stats.numChunksFailed.get();
    }

    @Override
    public int getNumFilesFailed() {
        return stats.numFilesFailed.get();
    }

    @Override
    public int getNumChunksSucceeded() {
        return stats.numChunksSucceeded.get();
    }

    @Override
    public int getNumFilesSucceeded() {
        return stats.numFilesSucceeded.get();
    }

    @Override
    public int getNumChunksSkipped() {
        return stats.numChunksSkipped.get();
    }

    @Override
    public int getNumChecksumsSucceeded() {
        return stats.numChecksumsSucceeded.get();
    }

    @Override
    public int getNumChecksumsFailed() {
        return stats.numChecksumsFailed.get();
    }

    @Override
    public String getLastDateRan() {
        return lastRunDate.toString();
    }

    @Override
    public int getSecondsSinceLastUpdate() {
        Seconds seconds = Seconds.secondsBetween(lastRunDate, new DateTime());
        return seconds.getSeconds();
    }
}
