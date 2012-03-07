package com.urbanairship.hbackup;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps counters of various kinds of successes and failures. The counter names are generic enough
 * to be used by both the backup tool and the checksum verifier for different purposes.
 */
public class Stats {
    public final AtomicInteger numUpToDateFilesSkipped = new AtomicInteger(0);
    public final AtomicInteger numChunksFailed = new AtomicInteger(0);
    public final AtomicInteger numFilesFailed = new AtomicInteger(0);
    public final AtomicInteger numChunksSucceeded = new AtomicInteger(0);
    public final AtomicInteger numFilesSucceeded = new AtomicInteger(0);
    public final AtomicInteger numChunksSkipped = new AtomicInteger(0);
    public final AtomicInteger numChecksumsSucceeded = new AtomicInteger(0);
    public final AtomicInteger numChecksumsFailed = new AtomicInteger(0);
    public final Queue<Exception> fileFailureExceptions = new ConcurrentLinkedQueue<Exception>();
}
