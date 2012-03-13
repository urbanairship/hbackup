package com.urbanairship.hbackup.service.tasks;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.urbanairship.hbackup.HBackup;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.service.ScheduledBackupStats;
import com.yammer.metrics.Metrics;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import com.yammer.metrics.core.Timer;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

/**
 */
public class HBackupScheduled extends AbstractScheduledService {

    private static final Logger log = LogManager.getLogger(HBackupScheduled.class);
    
    private final HBackupConfig config;
    private final ScheduledBackupStats backupStatsMBean = new ScheduledBackupStats();
    private final Timer timer = Metrics.newTimer(HBackup.class, "Backup");

    public HBackupScheduled(HBackupConfig config) {
        this.config = config;
    }


    @Override
    protected void runOneIteration() {
        try {
            log.info("Backup starting.");
            long start = System.nanoTime();

            HBackup backup = new HBackup(config);
            backup.runWithCheckedExceptions();

            backupStatsMBean.setStats(backup.getStats());
            timer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        } catch (Exception e) {
            log.error("Error performing backup : ", e);
        }
    }

    @Override
    protected void startUp() throws Exception {
        log.info("Starting up....");
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        platformMBeanServer.registerMBean(backupStatsMBean, new ObjectName("com.urbanairship.service:Type=Backup, name=Last Run"));
    }

    @Override
    protected void shutDown() throws Exception {
        log.info("Stopped.");
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, config.backupInterval, TimeUnit.HOURS);
    }
}
