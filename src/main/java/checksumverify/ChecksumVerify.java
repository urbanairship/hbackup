package checksumverify;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.urbanairship.hbackup.ChecksumService;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.Source;
import com.urbanairship.hbackup.SourceFile;

public class ChecksumVerify implements Runnable {
    private static final Logger log = LogManager.getLogger(ChecksumVerify.class);
    
    private final ChecksumService checksumService;
    private final Source source;
    private final ChecksumStats stats = new ChecksumStats();
    private final HBackupConfig config;
    
    public static void main(String[] args) throws Exception {
        HBackupConfig config = HBackupConfig.fromEnv(args);
        new ChecksumVerify(config).runWithCheckedExceptions();
        System.exit(0); // We'll reach here if and only if no exception was thrown above
    }
    
    public ChecksumVerify(HBackupConfig conf) throws IOException, URISyntaxException {
        checksumService = ChecksumService.forUri(new URI(conf.checksumUri), conf);
        URI dataUri = new URI(conf.from);
        if(!dataUri.getScheme().equals("s3")) {
            String msg = "Your \"from\" location " + conf.from + " has URI scheme " +
                    dataUri.getScheme() + " which can't be supported by the checksum verifier. We can " +
                    "only verify checksums for data in S3 because that's the only place checksums are " +
                    "stored.";
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
        source = Source.forUri(dataUri, conf);
        config = conf;
    }
    
    /**
     * 
     * @return whether all checksums were present and matched
     */
    public boolean runWithCheckedExceptions() throws IOException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(config.concurrentFiles, 
                config.concurrentFiles, Long.MAX_VALUE, TimeUnit.HOURS, 
                new LinkedBlockingQueue<Runnable>());
        executor.prestartAllCoreThreads();
        
        List<SourceFile> sourceFiles = source.getFiles(true);
        for(SourceFile file: sourceFiles) {
            int numChunks = (int)(file.getLength() / config.s3PartSize) + 1;
            long fileLen = file.getLength();
            
            ChecksumStateMachine fileChecksumStateMachine = new ChecksumStateMachine(file, numChunks, 
                    config.chunkRetries, checksumService, stats);

            log.debug("Queueing file " + file.getRelativePath() + " for checksumming in " +
                    numChunks + " chunks");
            
            for(int i=0; i<numChunks; i++) {
                long chunkStartOffset = i * config.s3PartSize;
                long chunkLen = Math.min(fileLen - chunkStartOffset, config.s3PartSize); 
                ChunkChecksummer chunkChecksummer = new ChunkChecksummer(file, chunkStartOffset, chunkLen,
                        config.chunkRetries, fileChecksumStateMachine);
                executor.execute(chunkChecksummer);
            }
        }
        log.debug("Main thread blocking on executor.shutdown()");
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for executor to finish", e);
        }
        
        int matchedChecksums = stats.matched.get();
        int mismatchedChecksums = stats.mismatched.get();
        int unreadableChecksums = stats.unreadableChecksums.get();
        int missingChecksums = stats.missingChecksums.get();
        int unreadableChunks = stats.unreadableChunks.get();
        int unreadableFiles = stats.unreadableFiles.get();
        int chunksSkipped = stats.chunksSkipped.get();
        
        log.info("Checksums that matched: " + matchedChecksums);
        log.info("Checksums that didn't match: " + mismatchedChecksums);
        log.info("Unreadable checksums: " + unreadableChecksums);
        log.info("Missing checksums: " + missingChecksums);
        log.info("Unreadable chunks: " + unreadableChunks);
        log.info("Unreadable files: " + unreadableFiles);
        log.info("Chunks skipped due errors in same file: " + chunksSkipped);
        
        if(matchedChecksums == sourceFiles.size() && 
                mismatchedChecksums == 0 && 
                unreadableChecksums == 0 && 
                missingChecksums == 0 && 
                unreadableChunks == 0 && 
                unreadableFiles == 0 && 
                chunksSkipped == 0) {
            log.debug("All checksums were verified");
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public void run() {
        try {
            runWithCheckedExceptions();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a ChecksumStats object with info about the run, including counters and a list of exceptions.
     */
    public ChecksumStats getStats() {
        return stats;
    }
}

