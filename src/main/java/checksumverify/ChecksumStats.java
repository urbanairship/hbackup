package checksumverify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ChecksumStats {
    public final AtomicInteger matched = new AtomicInteger(0);
    public final AtomicInteger mismatched = new AtomicInteger(0);
    public final AtomicInteger unreadableChecksums = new AtomicInteger(0);
    public final AtomicInteger missingChecksums = new AtomicInteger(0);
    public final AtomicInteger unreadableChunks = new AtomicInteger(0);
    public final AtomicInteger unreadableFiles = new AtomicInteger(0);
    public final AtomicInteger chunksSkipped = new AtomicInteger(0);

    public final List<IOException> workerExceptions = Collections.synchronizedList(new ArrayList<IOException>());
}
