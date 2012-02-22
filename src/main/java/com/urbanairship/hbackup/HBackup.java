package com.urbanairship.hbackup;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.urbanairship.hbackup.HBackupConfig.OptHelp;

// TODO:
//  Allow non-recursive S3 sources
//  Append mode for resuming failed files when writing to HDFS

/**
 * The main class and client API. Instantiate this class with a backup confguration and call run()
 * or runWithCheckedExceptions() to start backing up.
 */
public class HBackup implements Runnable {
    private static final Logger log = LogManager.getLogger(HBackup.class);
        
    private final Source source;
    private final Sink sink;
    private final ChecksumService checksumService;
    private final HBackupConfig conf;
    private final Stats stats;
    
    public HBackup(HBackupConfig conf) throws URISyntaxException, IOException {
        this.conf = conf;
        this.stats = new Stats();
        this.source = Source.forUri(new URI(conf.from), conf, stats);
        this.sink = Sink.forUri(new URI(conf.to), conf, stats);
        if(conf.checksumUri != null) {
            this.checksumService = ChecksumService.forUri(new URI(conf.checksumUri), conf);
        } else {
            this.checksumService = null;
        }
    }
    
    @Override
    public void run() {
        try {
            runWithCheckedExceptions();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public void runWithCheckedExceptions() throws IOException, InterruptedException {
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
        
        ThreadPoolExecutor executor = new ThreadPoolExecutor(conf.concurrentFiles, conf.concurrentFiles, 10, 
                TimeUnit.SECONDS, workQueue);
        
        Pattern p = null;
        if(conf.includePathsRegex != null) {
            log.debug("Using input path filter regex: " + conf.includePathsRegex);
            p = Pattern.compile(conf.includePathsRegex);
        } else {
            log.debug("Not configured for input path pattern matching, skipping");
        }
        
        // Consider all files in the source
        for (SourceFile file: source.getFiles(conf.recursive)) {
            // Copy the file unless it's up to date in the sink
            try {
                String relativePath = file.getRelativePath(); 
                
                // If regex file filtering is configured, check whether this file should be backed up
                if(p != null) {
                    if(!p.matcher(relativePath).matches()) {
                        log.debug("Skipping file " + relativePath + " because it didn't match regex " 
                                + conf.includePathsRegex);
                        continue;
                    }
                }
                
                if(sink.existsAndUpToDate(file)) {
                    log.debug("Skipping file since the target is up to date: " + file.getRelativePath());
                    stats.numUpToDateFilesSkipped.incrementAndGet();
                    continue;
                }
                
                // Ask the sink how the file should be chunked for transfer
                List<RetryableChunk> chunks = sink.getChunks(file);
                
                log.debug("Queueing file for transfer: " + file.getRelativePath());
                FileTransferState fileState = new FileTransferState(file, chunks.size(), stats);
                for(RetryableChunk chunk: chunks) {
                    // Enqueue each chunk for transfer
                    executor.execute(new ChunkRetryer(fileState, chunk, checksumService,
                            conf.chunkRetries, stats));
                }

            } catch (IOException e) {
                log.error("Skipping file " + file.getRelativePath() + " due to exception", e);
            }
        }
       
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        
        log.info("Files copied:  " + stats.numFilesSucceeded.get());
        log.info("Files skipped: " + stats.numUpToDateFilesSkipped.get());
        log.info("Files failed:  " + stats.numFilesFailed.get());
        log.info("Chunks copied: " + stats.numChunksSucceeded.get());
        log.info("Chunks failed: " + stats.numChunksFailed.get());
        
        // Re-throw the first exception seen by a worker thread, if any exceptions occurred
        if(!stats.fileFailureExceptions.isEmpty()) {
            throw new IOException("Re-throwing worker exception from main thread", 
                    stats.fileFailureExceptions.peek());
        }
    }
    
    public Stats getStats() {
        return stats;
    }

    public static void main(String[] args) throws Exception {
        HBackup hBackup = null;
        try {
            hBackup = new HBackup(HBackupConfig.fromEnv(args));
        } catch (IllegalArgumentException e) {
            log.error(e);
            System.err.println(usage());
            System.exit(1);
        }
        hBackup.runWithCheckedExceptions();

        if(hBackup.getStats().numFilesFailed.get() > 0) {
            log.error("Exiting with non-zero exit code because some copy failed");
            System.exit(1);
        } else {
            log.debug("No failure occurred, exiting with exit code 0");
            System.exit(0);
        }
    }
    
    public static String usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Usage: CLASSPATH=... java -Dprop=val -Dprop=val com.urbanairship.hbackup.HBackup [resource] [resource]\n");
        sb.append("The \"resource\"s are filenames or URLs pointing to properties files which may set config values.\n");
        sb.append("You can set config values in the resource files or by setting JVM system properties with -Dprop=val.\n");
        sb.append("\n");
        sb.append("The available config values are:\n");
        
        final int padNameTo = 32;

        for(OptHelp optHelp: HBackupConfig.optHelps) {
            sb.append(" ");
            sb.append(StringUtils.rightPad(optHelp.name, padNameTo));
            sb.append(optHelp.desc);
            if(optHelp.def != null) {
                sb.append(" (default " + optHelp.def + ")");
            }
            sb.append("\n");
        }
        
        sb.append("\n");
        sb.append("When specifying HDFS URIs, you can leave the host part blank (hdfs://dir/file.txt instead of hdfs://host:port/dir/file.txt) if " + 
                "the classpath contains a Hadoop configuration pointing to a default filesystem.\n");
        sb.append("\n");
        sb.append("Examples:\n");
        sb.append("  CLASSPATH=/mnt/services/tasktracker/etc/:.hbackup-0.9-jar-with-dependencies.jar java -Dhbackup.from=hdfs:///from -Dhbackup.to=hdfs:///to com.urbanairship.hbackup.HBackup ./otherconfigs.properties");
        return sb.toString();
    }
}
