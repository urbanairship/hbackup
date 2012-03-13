package com.urbanairship.hbackup.service;


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
public class BackupService {

    private static final Logger log = LogManager.getLogger(BackupService.class);

    public static void main(String... args) throws Exception {

        final HBackupConfig configuration = HBackupConfig.fromEnv(args);
        int backupInterval = configuration.backupInterval;

        if (backupInterval <= 0) {
            log.info(String.format("Backup interval less than or equal to 0, won't run, please set %s.",HBackupConfig.CONF_BACKUPINVERVAL));
            System.exit(1);
        }

        HBackupScheduled backupThreadService = new HBackupScheduled(configuration);
        backupThreadService.startAndWait();

        int staleCheckInterval = configuration.staleCheckInteveral;
        if (staleCheckInterval > 0) {
            StaleCheckScheduled staleCheckScheduled = new StaleCheckScheduled(configuration);
            staleCheckScheduled.startAndWait();
        }
    }
}
