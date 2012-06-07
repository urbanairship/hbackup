/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup.service;


/**
 * Expose some of the {@link ScheduledStaleCheckStats} in a jmx bean.
 */
public interface ScheduledStaleCheckStatsMXBean {

    public int getNonStaleFiles();
    public int getStaleFiles();
    public int getFailedFiles();
    public String getLastDateRun();
    public int getSecondsSinceLastUpdate();
}
