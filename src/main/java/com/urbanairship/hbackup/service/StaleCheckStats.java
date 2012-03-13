package com.urbanairship.hbackup.service;



import java.util.Date;

public class StaleCheckStats implements StalecheckStatsMXBean {

    private com.urbanairship.hbackup.StaleCheckStats stats = new com.urbanairship.hbackup.StaleCheckStats();
    private Date date = new Date();

    public void setStats(com.urbanairship.hbackup.StaleCheckStats stats) {
        this.stats = stats;
    }


    @Override
    public int getNonStaleFiles() {
       return stats.nonStaleFiles.get();
    }

    @Override
    public int getStaleFiles() {
        return stats.staleFiles.get();
    }

    @Override
    public int getFailedFiles() {
        return stats.failedFiles.get();
    }

    @Override
    public String getLastDateRun() {
        return date.toString();
    }


}
