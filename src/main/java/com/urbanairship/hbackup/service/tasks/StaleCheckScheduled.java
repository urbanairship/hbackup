package com.urbanairship.hbackup.service.tasks;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.StalenessCheck;
import com.urbanairship.hbackup.service.ScheduledStaleCheckStats;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

/**
 * Class to run the staleness check on a configured interval. Just instantiates the object and runs it, exposes
 * some stats via jmx about the output.
 */
public class StaleCheckScheduled  extends AbstractScheduledService {

    private static final Logger log = LogManager.getLogger(StaleCheckScheduled.class);

    private final HBackupConfig config;
    private final ScheduledStaleCheckStats staleCheckStats = new ScheduledStaleCheckStats();
    private final Timer timer = Metrics.newTimer(StalenessCheck.class, "Staleness Check");

    public StaleCheckScheduled(HBackupConfig config) {
        this.config = config;
    }

    @Override
    protected void runOneIteration() {
        try {
            log.info("Checking staleness.");
            long start = System.nanoTime();

            StalenessCheck stalenessCheck = new StalenessCheck(config);
            stalenessCheck.runWithCheckedExceptions();

            com.urbanairship.hbackup.StaleCheckStats stats = stalenessCheck.getStats();
            staleCheckStats.setStats(stats);
            timer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        } catch (Exception e) {
            log.error("Error checking staleness : ",e);
        }
    }

    @Override
    protected void startUp() throws Exception {
        log.info("Starting up....");
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        platformMBeanServer.registerMBean(staleCheckStats, new ObjectName("com.urbanairship.service:Type=Stale Check, name=Last Run"));
    }

    @Override
    protected void shutDown() throws Exception {
        log.info("Stopped.");
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, config.staleCheckInteveral, TimeUnit.MINUTES);
    }
}
