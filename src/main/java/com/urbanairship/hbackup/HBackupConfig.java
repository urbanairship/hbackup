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
import org.jets3t.service.model.MultipartUpload;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.MultipartUtils;

public class HBackupConfig {
    private static final Logger log = LogManager.getLogger(HBackupConfig.class);
    
    // Config keys 
    public static final String CONF_FROM = "hbackup.from";
    public static final String CONF_TO = "hbackup.to";
    public static final String CONF_CONCURRENTFILES = "hbackup.concurrentFiles";
    public static final String CONF_RECURSIVE = "hbackup.recursive";
    public static final String CONF_SOURCES3ACCESSKEY = "hbackup.from.s3AccessKey";
    public static final String CONF_SOURCES3SECRET = "hbackup.from.s3Secret";
    public static final String CONF_SINKS3ACCESSKEY = "hbackup.to.s3AccessKey";
    public static final String CONF_SINKS3SECRET = "hbackup.to.s3Secret";
    public static final String CONF_S3PARTSIZE= "hbackup.s3.partSize";
    public static final String CONF_S3MULTIPARTTHRESHOLD = "hbackup.s3.multipartThreshold";
    
    public static final int DEFAULT_CONCURRENT_FILES = 5;
    public static final long DEFAULT_S3_PART_SIZE = 20 * 1024 * 1024;
    public static final int DEFAULT_S3_MULTIPART_THRESHOLD = 100 * 1024 * 1024;
    
    // Config values
    public final String from;
    public final String to;
    public final int concurrentFiles;
    public final boolean recursive;
//    public final String sourceS3AccessKey;
//    public final String sourceS3Secret;
//    public final String sinkS3AccessKey;
//    public final String sinkS3Secret;
    public final AWSCredentials s3SourceCredentials;
    public final AWSCredentials s3SinkCredentials;
    public final long s3PartSize;
    public final long s3MultipartThreshold;
    public final org.apache.hadoop.conf.Configuration hadoopConf;
    
    public HBackupConfig(String from, String to, int concurrentFiles, boolean recursive, 
            String sourceS3AccessKey, String sourceS3Secret, String sinkS3AccessKey, String sinkS3Secret,
            long s3PartSize, long s3MultipartThreshold, 
            org.apache.hadoop.conf.Configuration hadoopConf) {
        if(from == null || to == null) {
            throw new IllegalArgumentException("from and to cannot be null");
        }
        
        if(s3PartSize < MultipartUtils.MIN_PART_SIZE || s3PartSize > MultipartUtils.MAX_OBJECT_SIZE) {
            throw new IllegalArgumentException("s3PartSize must be within the range " + 
                    MultipartUtils.MIN_PART_SIZE + " to " + MultipartUtils.MAX_OBJECT_SIZE);
        }
        
        if(s3MultipartThreshold < s3PartSize) {
            throw new IllegalArgumentException("s3MultipartThreshold must be >= s3PartSize");
        }
        
        this.from = from;
        this.to = to;
        this.concurrentFiles = concurrentFiles;
        this.recursive = recursive;
//        this.sourceS3AccessKey = sourceS3AccessKey;
//        this.sourceS3Secret = sourceS3Secret;
//        this.sinkS3AccessKey = sinkS3AccessKey;
//        this.sinkS3Secret = sinkS3Secret;
        this.s3PartSize = s3PartSize;
        this.s3MultipartThreshold = s3MultipartThreshold;
        this.hadoopConf = hadoopConf;
        
        if(sourceS3AccessKey != null && sourceS3Secret != null) {
            this.s3SourceCredentials = new AWSCredentials(sourceS3AccessKey, sourceS3Secret);
        } else {
            this.s3SourceCredentials = null;
        }
        
        if(sinkS3AccessKey != null && sinkS3Secret != null) {
            this.s3SinkCredentials = new AWSCredentials(sinkS3AccessKey, sinkS3Secret);
        } else {
            this.s3SinkCredentials = null;
        }
    }

    /**
     * Get config with defaults for all params. "From" and "to" have no defaults. S3 keys will be parsed
     * from the system properties, or null. Hadoop config will be the default (parse normal Hadoop config 
     * files from the classpath).
     */
    public static HBackupConfig forTests(String from, String to) {
        SystemConfiguration sysProps = new SystemConfiguration();
        return new HBackupConfig(from, 
                to, 
                DEFAULT_CONCURRENT_FILES, 
                true, 
                sysProps.getString(CONF_SOURCES3ACCESSKEY), 
                sysProps.getString(CONF_SOURCES3SECRET), 
                sysProps.getString(CONF_SINKS3ACCESSKEY), 
                sysProps.getString(CONF_SINKS3SECRET),
                DEFAULT_S3_PART_SIZE, 
                DEFAULT_S3_MULTIPART_THRESHOLD, 
                new org.apache.hadoop.conf.Configuration());
    }

    /**
     * Load service configuration by loading resources (files, URLs) passed as command line args, per
     * Urban Airship convention. Also, JVM system properties will override anything configured elsewhere.
     */
    public static HBackupConfig fromEnv(String[] cmdLineArgs) {
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
        
        return new HBackupConfig(conf.getString(CONF_FROM), 
                conf.getString(CONF_TO), 
                conf.getInt(CONF_CONCURRENTFILES, DEFAULT_CONCURRENT_FILES), 
                conf.getBoolean(CONF_RECURSIVE),
                conf.getString(CONF_SOURCES3ACCESSKEY), 
                conf.getString(CONF_SOURCES3SECRET), 
                conf.getString(CONF_SINKS3ACCESSKEY), 
                conf.getString(CONF_SINKS3SECRET), 
                conf.getLong(CONF_S3PARTSIZE, DEFAULT_S3_PART_SIZE),
                conf.getLong(CONF_S3MULTIPARTTHRESHOLD, DEFAULT_S3_MULTIPART_THRESHOLD),
                new org.apache.hadoop.conf.Configuration(true));
    }
}
