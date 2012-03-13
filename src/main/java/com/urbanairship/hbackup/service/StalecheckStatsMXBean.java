package com.urbanairship.hbackup.service;


/**
 * Expose some of the {@link StaleCheckStats} in a jmx bean.
 */
public interface StalecheckStatsMXBean {

    public int getNonStaleFiles();
    public int getStaleFiles();
    public int getFailedFiles();
    public String getLastDateRun();
}
