package com.urbanairship.hbackup;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * The main class and client API. Instantiate this class with a backup confguration and call run()
 * or runWithCheckedExceptions() to start backing up.
 */
public class HBackup implements Runnable {
    private static final Logger log = LogManager.getLogger(HBackup.class);
        
    private final Source source;
    private final Sink sink;
    private final HBackupConfig conf;
    private final Stats stats;
    
    public HBackup(HBackupConfig conf) throws URISyntaxException, IOException {
        this.conf = conf;
        this.stats = new Stats();
        this.source = Source.forUri(new URI(conf.from), conf, stats);
        this.sink = Sink.forUri(new URI(conf.to), conf, stats);
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
        executor.prestartAllCoreThreads();
        
        // Consider all files in the source
        for (HBFile file: source.getFiles(conf.recursive)) {
            // Copy the file unless it's up to date in the sink
            try {
                if(!sink.existsAndUpToDate(file)) {
                    log.debug("Queueing file for transfer: " + file.getRelativePath());
                    // Ask the sink how the file should be chunked for transfer
                    for(Runnable r: sink.getChunks(file)) {
                        executor.execute(r);
                    }
                } else {
                    log.debug("Skipping file since the target is up to date: " + file.getRelativePath());
                    stats.numUpToDateFilesSkipped.incrementAndGet();
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
        if(!stats.transferExceptions.isEmpty()) {
            throw new IOException("Re-throwing worker exception from main thread", 
                    stats.transferExceptions.peek());
        }
    }
    
    public Stats getStats() {
        return stats;
    }
    
//    /**
//     * These objects go into the work queue to be executed by the thread pool executor. Their only
//     * purpose is to invoke a Sink when they're run().
//     */
//    private class SinkRunner implements Runnable {
//        private final HBFile file;
//        private final Sink sink;
//        
//        public SinkRunner(HBFile file, Sink sink) {
//            this.file = file;
//            this.sink = sink;
//        }
//        
//        @Override
//        public void run() {
//            try {
//                sink.write(file);
//                numCopied.incrementAndGet();
//            } catch (RuntimeException e) {
//                numFailed.incrementAndGet();
//                log.error("Runtime exception in sink", e);
//                workerException.compareAndSet(null, e);
//                throw e;
//            } catch (Exception e) {
//                numFailed.incrementAndGet();
//                workerException.compareAndSet(null, e);
//                log.error("Exception when copying", e);
//            } 
//        }
//    }
    
    public static void main(String[] args) throws Exception {
      HBackup hBackup = new HBackup(HBackupConfig.fromEnv(args));
      hBackup.runWithCheckedExceptions();
      
      if(hBackup.getStats().numFilesFailed.get() > 0) {
          log.error("Exiting with non-zero exit code because some copy failed");
          System.exit(1);
      } else {
          log.debug("No failure occurred, exiting with exit code 0");
          System.exit(0);
      }
  }
}
