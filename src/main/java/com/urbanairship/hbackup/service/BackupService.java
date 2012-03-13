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
            log.info("Backup interval less than or equal to 0, won't run");
            System.exit(0);
        }

        HBackupScheduled backupThreadService = new HBackupScheduled(configuration);
        backupThreadService.startAndWait();

        int staleCheckInteveral = configuration.staleCheckInteveral;
        if (staleCheckInteveral > 0) {
            StaleCheckScheduled staleCheckScheduled = new StaleCheckScheduled(configuration);
            staleCheckScheduled.startAndWait();
        }
    }
}
