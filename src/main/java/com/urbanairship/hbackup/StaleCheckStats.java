/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup;

import java.util.concurrent.atomic.AtomicInteger;

public class StaleCheckStats {
    public final AtomicInteger nonStaleFiles = new AtomicInteger(0);
    public final AtomicInteger staleFiles = new AtomicInteger(0);
    public final AtomicInteger failedFiles = new AtomicInteger(0);
    
    
}
