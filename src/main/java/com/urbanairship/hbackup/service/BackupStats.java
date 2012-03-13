package com.urbanairship.hbackup.service;

import com.urbanairship.hbackup.Stats;

import java.util.Date;

/**
 */
public class BackupStats implements BackupStatsMXBean {

    
    private Stats stats = new Stats();
    private Date date = new Date();
    
    public void setStats(Stats stats) {
        this.stats = stats;
        this.date = new Date();
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
        return date.toString();
    }
}
