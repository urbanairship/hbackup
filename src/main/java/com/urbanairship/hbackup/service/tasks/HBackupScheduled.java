/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup.service.tasks;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.urbanairship.hbackup.HBackup;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.Stats;
import com.urbanairship.hbackup.service.ScheduledBackupStats;
import com.yammer.metrics.Metrics;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import com.yammer.metrics.core.TimerMetric;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

/**
 */
public class HBackupScheduled extends AbstractScheduledService {

    private static final Logger log = LogManager.getLogger(HBackupScheduled.class);
    
    private final HBackupConfig config;
    private final ScheduledBackupStats backupStatsMBean = new ScheduledBackupStats();
    private final TimerMetric timer = Metrics.newTimer(HBackup.class, "Backup");
    private final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();

    private Stats lastRunStats = new Stats();

    private ObjectName jmxName;


    public HBackupScheduled(HBackupConfig config) {
        this.config = config;
        try {
            this.jmxName = new ObjectName("com.urbanairship.service:Type=Backup, name=Last Run");
        } catch (MalformedObjectNameException e) {
            log.error("Error registering jmx bean : ",e);
        }
    }


    @Override
    protected void runOneIteration() {
        try {
            log.info("Backup starting.");
            long start = System.nanoTime();

            HBackup backup = new HBackup(config);
            backup.runWithCheckedExceptions();

            lastRunStats = backup.getStats();
            backupStatsMBean.setStats(lastRunStats);
            timer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        } catch (Exception e) {
            log.error("Error performing backup : ", e);
        }
    }

    @Override
    protected void startUp() throws Exception {
        log.info("Starting up....");
        platformMBeanServer.registerMBean(backupStatsMBean, jmxName);
    }

    @Override
    protected void shutDown() throws Exception {
        platformMBeanServer.unregisterMBean(jmxName);
        log.info("Stopped.");
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, config.backupIntervalMinutes, TimeUnit.MINUTES);
    }

    public Stats getLastRunStats() {
        return lastRunStats;
    }

    public TimerMetric getTimerMetric() {
        return timer;
    }
}
