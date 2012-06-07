/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class StalenessCheck {
    private static final Logger log = LogManager.getLogger(StalenessCheck.class);
    
    private final HBackupConfig config;
    private final Source source;
    private final Sink sink;
    private final StaleCheckStats stats = new StaleCheckStats();
    
    public StalenessCheck(HBackupConfig config) throws IOException, URISyntaxException {
        this.config = config;
        this.source = Source.forUri(new URI(config.from), config);
        this.sink = Sink.forUri(new URI(config.to), config, new Stats());
    }
    
    public int runWithCheckedExceptions() throws IOException, InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(config.concurrentFiles, 
                config.concurrentFiles, Long.MAX_VALUE, TimeUnit.SECONDS, 
                new LinkedBlockingQueue<Runnable>());
        
        List<SourceFile> sourceFiles = source.getFiles(true);
        if(sourceFiles.size() == 0) {
            log.error("Returning non-zero since there were no files in the source.");
            return 1;
        }
        for(SourceFile file: sourceFiles) {
            log.debug("Enqueueing staleness check for file " + file.getRelativePath());
            executor.execute(new FileMtimeComparer(file, sink, config, stats));
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        
        int numFreshFiles = stats.nonStaleFiles.get();
        int numStaleFiles = stats.staleFiles.get();
        int numFilesFailed = stats.failedFiles.get();
        
        log.info("Up to date files: " + numFreshFiles);
        log.info("Stale files: " + numStaleFiles);
        log.info("Files that couldn't be checked: " + numFilesFailed);
        
        if(numFreshFiles == sourceFiles.size() && numStaleFiles == 0 && numFilesFailed == 0) {
            log.debug("Returning zero since all files were up-to-date.");
            return 0;
        } else {
            log.warn("Returning non-zero. Either there were errors or something was stale.");
            return 2;
        }
    }
    
    public StaleCheckStats getStats() {
        return stats;
    }
    
    public static void main(String[] args) throws Exception {
        HBackupConfig hbackupConfig = HBackupConfig.fromEnv(args);
        try {
            int rc = new StalenessCheck(hbackupConfig).runWithCheckedExceptions();
            System.exit(rc);
        } catch (IllegalArgumentException e) {
            log.error(e);
            System.err.println(HBackup.usage());
            System.exit(1);
        }
    }
}
