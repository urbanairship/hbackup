/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup.service;



import com.urbanairship.hbackup.StaleCheckStats;
import org.joda.time.DateTime;
import org.joda.time.Seconds;


public class ScheduledStaleCheckStats implements ScheduledStaleCheckStatsMXBean {

    private StaleCheckStats stats = new StaleCheckStats();
    private DateTime lastRunDate = new DateTime();

    public void setStats(com.urbanairship.hbackup.StaleCheckStats stats) {
        this.stats = stats;
        this.lastRunDate = new DateTime();
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
        return lastRunDate.toString();
    }

    @Override
    public int getSecondsSinceLastUpdate() {
        Seconds seconds = Seconds.secondsBetween(lastRunDate,new DateTime());
        return seconds.getSeconds();
    }


}
