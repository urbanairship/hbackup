package com.urbanairship.hbackup;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Stats {
    public final AtomicInteger numUpToDateFilesSkipped = new AtomicInteger(0);
    public final AtomicInteger numChunksFailed = new AtomicInteger(0);
    public final AtomicInteger numFilesFailed = new AtomicInteger(0);
    public final AtomicInteger numChunksSucceeded = new AtomicInteger(0);
    public final AtomicInteger numFilesSucceeded = new AtomicInteger(0);
    public final AtomicInteger numChunksSkipped = new AtomicInteger(0);
    
    public Queue<Exception> transferExceptions = new ConcurrentLinkedQueue<Exception>(); 
}
