package com.urbanairship.hbackup;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HBackupConfig {
    private static final Logger log = LogManager.getLogger(HBackupConfig.class);
    
    // Config keys 
    public static final String CONF_FROM = "hbackup.from";
    public static final String CONF_TO = "hbackup.to";
    public static final String CONF_CONCURRENTFILES = "hbackup.concurrentFiles";
    public static final String CONF_RECURSIVE = "hbackup.recursive";
    public static final String CONF_S3ACCESSKEY = "hbackup.s3AccessKey";
    public static final String CONF_S3SECRET = "hbackup.s3Secret";
    
    public static final int DEFAULT_CONCURRENT_FILES = 5;
    
    // Config values
    public final String from;
    public final String to;
    public final int concurrentFiles;
    public final boolean recursive;
    public final String s3AccessKey;
    public final String s3Secret;
    public final org.apache.hadoop.conf.Configuration hadoopConf;
    
    public HBackupConfig(String from, String to, int concurrentFiles, boolean recursive, 
            String s3AccessKey, String s3Secret, org.apache.hadoop.conf.Configuration hadoopConf) {
        this.from = from;
        this.to = to;
        this.concurrentFiles = concurrentFiles;
        this.recursive = recursive;
        this.s3AccessKey = s3AccessKey;
        this.s3Secret = s3Secret;
        this.hadoopConf = hadoopConf;
    }

    /**
     * Get config with defaults for all params. "From" and "to" have no defaults. S3 keys will be null.
     * Hadoop config will be the default (parse normal Hadoop config files from the classpath).
     */
    public HBackupConfig(String from, String to) {
        this(from, to, DEFAULT_CONCURRENT_FILES, true, null, null, new org.apache.hadoop.conf.Configuration());
    }

    /**
     * Load service configuration by loading resources (files, URLs) passed as command line args, per
     * Urban Airship convention. Also, JVM system properties will override anything configured elsewhere.
     */
    public static HBackupConfig fromCmdLineArgs(String[] cmdLineArgs) {
        CompositeConfiguration conf = new CompositeConfiguration();
        for(String arg : cmdLineArgs) {
            log.info("Processing configuration resource at " + arg);
            try {

                //first see if the properties loader can make sense of things
                try {
                    PropertiesConfiguration pc = new PropertiesConfiguration(arg);
                    conf.addConfiguration(pc);
                    continue;
                }
                catch(Exception ex) {
                    log.debug("Properties configuration failed to load " + arg);
                }

                //now try and treat the resource as a file explicitly
                URL url = null;

                File file = new File(arg);
                if(file.exists()) {
                    url = file.toURI().toURL();
                    conf.addConfiguration(new PropertiesConfiguration(url));
                    continue;
                }

                //didn't succeed in loading from the classpath unqualified, try a more elaborate model
                try {
                    url = Thread.currentThread().getContextClassLoader().getResource(arg);
                    conf.addConfiguration(new PropertiesConfiguration(url));
                    continue;
                }
                catch(Exception e) {
                    log.info("Failed to load " + arg + " from classpath.");
                }

                if(url == null) {
                    //last ditch, try as a fully qualified URL
                    url = new URL(arg);
                    conf.addConfiguration(new PropertiesConfiguration(url));
                    continue;
                }
            }
            catch(ConfigurationException ce) {
                log.warn("Invalid configuration source '" + arg + "', ignoring.", ce);
            }
            catch(MalformedURLException mue) {
                log.warn("Invalid configuration URL '" + arg + "', ignoring.", mue);
            }
        }

        //system props override anything in the files
        conf.addConfiguration(new SystemConfiguration());
        
        return new HBackupConfig(conf.getString(CONF_FROM), conf.getString(CONF_TO), 
                conf.getInt(CONF_CONCURRENTFILES, DEFAULT_CONCURRENT_FILES), conf.getBoolean(CONF_RECURSIVE),
                conf.getString(CONF_S3ACCESSKEY), conf.getString(CONF_S3SECRET), 
                new org.apache.hadoop.conf.Configuration(true));
    }
}
