package com.urbanairship.hbackup.service;


import com.google.common.util.concurrent.AbstractIdleService;
import com.urbanairship.hbackup.*;
import com.urbanairship.hbackup.service.tasks.HBackupScheduled;
import com.urbanairship.hbackup.service.tasks.StaleCheckScheduled;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Class that runs the {@link HBackup} and {@link StalenessCheck} on a regular interval as defined by
 *
 * -Dhbackup.interval=<hour interval>
 * and
 * -Dhbackup.stalecheck.interval=<hour interval>
 *
 */
public class BackupService extends AbstractIdleService {

    private static final Logger log = LogManager.getLogger(BackupService.class);


    private final HBackupConfig configuration;

    private HBackupScheduled backupThreadService;
    private StaleCheckScheduled staleCheckScheduled;

    public static void main(String... args) throws Exception {

        HBackupConfig config = HBackupConfig.fromEnv(args);
        int backupInterval = config.backupIntervalMinutes;

        if (backupInterval <= 0) {
            log.info(String.format("Backup interval less than or equal to 0, won't run, please set %s.",HBackupConfig.CONF_BACKUPINTERVAL));
            System.exit(1);
        }

        BackupService backupService = new BackupService(config);
        backupService.startAndWait();
    }

    public BackupService(HBackupConfig configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void startUp() throws Exception {
        backupThreadService = new HBackupScheduled(configuration);
        backupThreadService.startAndWait();
        int staleCheckInterval = configuration.staleCheckIntervalMinutes;
        if (staleCheckInterval > 0) {
            staleCheckScheduled = new StaleCheckScheduled(configuration);
            staleCheckScheduled.startAndWait();
        }
    }

    @Override
    protected void shutDown() throws Exception {
        backupThreadService.stopAndWait();
        staleCheckScheduled.stopAndWait();
    }

}
