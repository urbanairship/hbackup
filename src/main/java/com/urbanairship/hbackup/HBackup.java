package com.urbanairship.hbackup;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
    
    private final AtomicInteger numSkippedUpToDate = new AtomicInteger(0);
    private final AtomicInteger numCopied = new AtomicInteger(0);
    private final AtomicInteger numFailed = new AtomicInteger(0);
    private final AtomicReference<Exception> workerException = new AtomicReference<Exception>(null);
    
    public HBackup(HBackupConfig conf) throws URISyntaxException, IOException {
        this.conf = conf;
        this.source = Source.forUri(new URI(conf.from), conf);
        this.sink = Sink.forUri(new URI(conf.to), conf);
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
    
        for (HBFile file: source.getFiles(conf.recursive)) {
            if(!sink.existsAndUpToDate(file)) {
                log.debug("Queueing file for transfer: " + file.getRelativePath());
                executor.execute(new SinkRunner(file, sink));
            } else {
                log.debug("Skipping file since the target is up to date: " + file.getRelativePath());
                numSkippedUpToDate.incrementAndGet();
            }
        }
       
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        
        log.info("Files copied:  " + numCopied.get());
        log.info("Files failed:  " + numFailed.get());
        log.info("Files skipped: " + numSkippedUpToDate.get());
        
        // Re-throw the first exception seen by a worker thread, if any exceptions occurred
        Exception ex = workerException.get();
        if(ex != null) {
            throw new IOException("Re-throwing worker exception from main thread", ex);
        }
    }
    
    public int numFailed() {
        return numFailed.get();
    }
    
    /**
     * These objects go into the work queue to be executed by the thread pool executor. Their only
     * purpose is to invoke a Sink when they're run().
     */
    private class SinkRunner implements Runnable {
        private final HBFile file;
        private final Sink sink;
        
        public SinkRunner(HBFile file, Sink sink) {
            this.file = file;
            this.sink = sink;
        }
        
        @Override
        public void run() {
            try {
                sink.write(file);
                numCopied.incrementAndGet();
            } catch (RuntimeException e) {
                numFailed.incrementAndGet();
                log.error("Runtime exception in sink", e);
                workerException.compareAndSet(null, e);
                throw e;
            } catch (Exception e) {
                numFailed.incrementAndGet();
                workerException.compareAndSet(null, e);
                log.error("Exception when copying", e);
            } 
        }
    }
    
    public static void main(String[] args) throws Exception {
      HBackup hBackup = new HBackup(HBackupConfig.fromEnv(args));
      hBackup.runWithCheckedExceptions();
      
      if(hBackup.numFailed() > 0) {
          log.error("Exiting with non-zero exit code because some copy failed");
          System.exit(1);
      } else {
          log.debug("No failure occurred, exiting with exit code 0");
          System.exit(0);
      }
  }
}
