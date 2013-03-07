/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup.service.tasks;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.StaleCheckStats;
import com.urbanairship.hbackup.StalenessCheck;
import com.urbanairship.hbackup.service.ScheduledStaleCheckStats;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.TimerMetric;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class to run the staleness check on a configured interval. Just instantiates the object and runs it, exposes
 * some stats via jmx about the output.
 */
public class StaleCheckScheduled  extends AbstractScheduledService {

    private static final Logger log = LogManager.getLogger(StaleCheckScheduled.class);

    private final HBackupConfig config;
    private final ScheduledStaleCheckStats staleCheckStats = new ScheduledStaleCheckStats();
    private final TimerMetric timer = Metrics.newTimer(StalenessCheck.class, "Staleness Check");
    private final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    private final AtomicBoolean checking = new AtomicBoolean(false);

    private StaleCheckStats lastRunStats = new StaleCheckStats();

    private ObjectName jmxName;

    public StaleCheckScheduled(HBackupConfig config) {
        this.config = config;
        try {
            this.jmxName = new ObjectName("com.urbanairship.service:Type=Stale Check, name=Last Run");
        } catch (MalformedObjectNameException e) {
            log.error("Error registering jxm bean : ",e);
        }
    }

    @Override
    protected void runOneIteration() {
        try {
            if (!checking.compareAndSet(false, true)) {
                throw new RuntimeException("Staleness check already in progress");
            }

            log.info("Checking staleness.");
            long start = System.nanoTime();

            StalenessCheck stalenessCheck = new StalenessCheck(config);
            stalenessCheck.runWithCheckedExceptions();

            lastRunStats = stalenessCheck.getStats();
            staleCheckStats.setStats(lastRunStats);
            timer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        } catch (Exception e) {
            log.error("Error checking staleness : ",e);
        } finally {
            // If we didn't set here, the exception above was thrown when attempting to set checking to true.
            checking.compareAndSet(true, false);
        }
    }

    @Override
    protected void startUp() throws Exception {
        log.info("Starting up....");
        platformMBeanServer.registerMBean(staleCheckStats, jmxName);
    }

    @Override
    protected void shutDown() throws Exception {
        log.info("Waiting for check thread to exit");
        while (checking.get()) {
            Thread.sleep(1000);
        }
        platformMBeanServer.unregisterMBean(jmxName);
        log.info("Stopped.");
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, config.staleCheckIntervalMinutes, TimeUnit.MINUTES);
    }
    
    public StaleCheckStats getLastRunStats() {
        return lastRunStats;
    }
    
    public TimerMetric getTimerMetric() {
        return timer;
    }
}
