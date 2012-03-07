package com.urbanairship.hbackup;

import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * These are the Runnables that go in the executor queue used by the backup staleness checker util.
 */
public class FileMtimeComparer implements Runnable {
    private static final Logger log = LogManager.getLogger(FileMtimeComparer.class);
    
    private final HBackupConfig conf;
    private final Sink sink;
    private final SourceFile sourceFile;
    private final StaleCheckStats stats;
    
    public FileMtimeComparer(SourceFile sourceFile, Sink sink, HBackupConfig conf, 
            StaleCheckStats stats) {
        this.conf = conf;
        this.sink = sink;
        this.sourceFile = sourceFile;
        this.stats = stats;
    }
    
    @Override
    public void run() {
        String relativePath = sourceFile.getRelativePath();
        Long sourceMtime = getSourceMtime();
        if(sourceMtime == null) {
            stats.failedFiles.incrementAndGet();
            log.error("Failed getting source mtime for " + relativePath);
            return;
        }
        log.debug("Got source mtime " + sourceMtime + " for file " + relativePath);
        
        Long sinkMTime = getSinkMtime();
        if(sinkMTime == null) {
            stats.failedFiles.incrementAndGet();
            log.error("Failed getting sink mtime for " + relativePath);
        }
        log.debug("Got sink mtime " + sinkMTime + " for file " + relativePath);
        
        if(sinkMTime < sourceMtime - conf.stalenessMillis) {
            log.info("File is stale: " + relativePath);
            stats.staleFiles.incrementAndGet();
        } else {
            log.info("File is up to date (not stale): " + relativePath);
            stats.nonStaleFiles.incrementAndGet();
        }
    }
    
    /**
     * @return the source file's mtime, or null if all retries were exhausted.
     */
    public Long getSourceMtime() {
        int retriesRemaining = conf.numRetries; 
        
        do {
            try {
                return sourceFile.getMTime();
            } catch (IOException e) {
                log.error("Couldn't get source file mtime for " + sourceFile.getRelativePath(), e);
            }
        } while (retriesRemaining-- > 0);
        log.error("All retries exhausted getting source file mtime for " + sourceFile.getRelativePath());
        return null;
    }
    
    /**
     * @return the sink file's mtime, or null if all retries were exhausted.
     */
    public Long getSinkMtime() {
        int retriesRemaining = conf.numRetries; 
        do {
            try {
                return sink.getMTime(sourceFile.getRelativePath());
            } catch (IOException e) {
                log.error("Couldn't get sink file mtime for " + sourceFile.getRelativePath(), e);
            }
        } while (retriesRemaining-- > 0);
        log.error("All retries exhausted getting source file mtime for " + sourceFile.getRelativePath());
        return null;
    }
      
}
