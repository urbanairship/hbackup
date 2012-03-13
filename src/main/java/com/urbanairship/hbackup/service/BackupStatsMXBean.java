package com.urbanairship.hbackup.service;


/**
 * Expose some stats via jmx for the backup.
 */
public interface BackupStatsMXBean {

    public int getNumUpToDateFilesSkipped();
    public int getNumChunksFailed();
    public int getNumFilesFailed();
    public int getNumChunksSucceeded();
    public int getNumFilesSucceeded();
    public int getNumChunksSkipped();
    public int getNumChecksumsSucceeded();
    public int getNumChecksumsFailed();
    public String getLastDateRan();
}
