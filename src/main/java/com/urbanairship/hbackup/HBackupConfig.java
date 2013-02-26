/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.MultipartUtils;

public class HBackupConfig {
    private static final Logger log = LogManager.getLogger(HBackupConfig.class);
    
    // Config keys 
    public static final String CONF_FROM = "hbackup.from";
    public static final String CONF_TO = "hbackup.to";
    public static final String CONF_CONCURRENTCHUNKS = "hbackup.concurrentChunks";
    public static final String CONF_RECURSIVE = "hbackup.recursive";
    public static final String CONF_SOURCES3ACCESSKEY = "hbackup.from.s3AccessKey";
    public static final String CONF_SOURCES3SECRET = "hbackup.from.s3Secret";
    public static final String CONF_SINKS3ACCESSKEY = "hbackup.to.s3AccessKey";
    public static final String CONF_SINKS3SECRET = "hbackup.to.s3Secret";
    public static final String CONF_S3PARTSIZE = "hbackup.s3.partSize";
    public static final String CONF_S3MULTIPARTTHRESHOLD = "hbackup.s3.multipartThreshold";
    public static final String CONF_MTIMECHECK = "hbackup.mtimecheck";
    public static final String CONF_INCLUDEPATHSREGEX = "hbackup.includePathsRegex";
    public static final String CONF_CHECKSUMURI = "hbackup.checksumUri";
    public static final String CONF_CHUNKRETRIES = "hbackup.chunkRetries";
    public static final String CONF_CHECKSUMS3ACCESSKEY = "hbackup.checksum.s3AccessKey";
    public static final String CONF_CHECKSUMS3SECRET = "hbackup.checksum.s3Secret";
    public static final String CONF_FALLBACKS3ACCESSKEY = "hbackup.s3AccessKey";
    public static final String CONF_FALLBACKS3SECRET = "hbackup.s3Secret";
    public static final String CONF_STALEMILLIS = "hbackup.staleMillis";
    public static final String CONF_BACKUPINTERVAL = "hbackup.intervalMins";
    public static final String CONF_STALECHECKINTERVAL = "hbackup.stalecheck.intervalMins";
    public static final String CONF_MINIMUM_MTIMEMILLIS = "hbackup.minimumMtimeMillis";

    public static final int DEFAULT_CONCURRENT_FILES = 5;
    public static final long DEFAULT_S3_PART_SIZE = 100 * 1024 * 1024;
    public static final int DEFAULT_S3_MULTIPART_THRESHOLD = 100 * 1024 * 1024;
    public static final boolean DEFAULT_MTIMECHECK = true;
    public static final boolean DEFAULT_RECURSIVE = true;
    public static final int DEFAULT_CHUNKRETRIES = 4;
    public static final long DEFAULT_STALEMILLIS = TimeUnit.DAYS.toMillis(1);
    public static final long DEFAULT_MINIMUM_MTIMEMILLIS = TimeUnit.DAYS.toMillis(1);

    // Config values
    public final String from;
    public final String to;
    public final int concurrentFiles;
    public final boolean recursive;
    public final AWSCredentials s3SourceCredentials;
    public final AWSCredentials s3SinkCredentials;
    public final AWSCredentials s3ChecksumCredentials;
    public final long s3PartSize;
    public final long s3MultipartThreshold;
    public final org.apache.hadoop.conf.Configuration hdfsSourceConf;
    public final org.apache.hadoop.conf.Configuration hdfsSinkConf;
    public final boolean mtimeCheck;
    public final String includePathsRegex;
    public final String checksumUri;
    public final int numRetries;
    public final long stalenessMillis;
    public final int backupIntervalMinutes;
    public final int staleCheckIntervalMinutes;
    public final long minimumMtimeMillis;

    /**
     * See {@link #optHelps} for an explanation of the parameters.
     */
    public HBackupConfig(String from, String to, int concurrentFiles, boolean recursive, 
            String sourceS3AccessKey, String sourceS3Secret, String sinkS3AccessKey, String sinkS3Secret,
            long s3PartSize, long s3MultipartThreshold, Configuration hdfsSourceConf, 
            Configuration hdfsSinkConf, boolean mtimeCheck, String includePathsRegex, 
            String checksumUri, int chunkRetries, String checksumS3AccessKey, String checksumS3Secret,
            String fallbackS3AccessKey, String fallbackS3Secret, long staleMillis, int backupIntervalMinutes,
            int staleCheckIntervalMinutes, long minimumMtimeMillis) {
        
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
        this.s3PartSize = s3PartSize;
        this.s3MultipartThreshold = s3MultipartThreshold;
        this.hdfsSourceConf = hdfsSourceConf;
        this.hdfsSinkConf = hdfsSinkConf;
        this.mtimeCheck = mtimeCheck;
        this.includePathsRegex = includePathsRegex;
        this.checksumUri = checksumUri;
        this.numRetries = chunkRetries;
        this.stalenessMillis = staleMillis;
        this.backupIntervalMinutes = backupIntervalMinutes;
        this.staleCheckIntervalMinutes = staleCheckIntervalMinutes;
        this.minimumMtimeMillis = minimumMtimeMillis;

        // The fallback credentials are used whever the config doesn't specify specific credentials
        // for source/sink/checksum. This makes the common case easy, where there is only one set
        // of credentials used for all three things.
        AWSCredentials fallbackAwsCreds = null;
        if(fallbackS3AccessKey != null && fallbackS3Secret != null) {
            fallbackAwsCreds = new AWSCredentials(fallbackS3AccessKey, fallbackS3Secret);
        }
        
        if(sourceS3AccessKey != null && sourceS3Secret != null) {
            this.s3SourceCredentials = new AWSCredentials(sourceS3AccessKey, sourceS3Secret);
        } else {
            this.s3SourceCredentials = fallbackAwsCreds;
        }
        
        if(sinkS3AccessKey != null && sinkS3Secret != null) {
            this.s3SinkCredentials = new AWSCredentials(sinkS3AccessKey, sinkS3Secret);
        } else {
            this.s3SinkCredentials = fallbackAwsCreds;
        }

        if(checksumS3AccessKey != null && checksumS3Secret!= null) {
            this.s3ChecksumCredentials = new AWSCredentials(checksumS3AccessKey, checksumS3Secret);
        } else {
            this.s3ChecksumCredentials = fallbackAwsCreds;
        }
    }

    /**
     * Get config with defaults for all params. "From" and "to" have no defaults. S3 keys will be parsed
     * from the system properties, or null. Hadoop config will be the default (parse normal Hadoop config 
     * files from the classpath).
     */
    public static HBackupConfig forTests(String from, String to, Configuration hdfsConf) {
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
                hdfsConf,
                hdfsConf,
                true,
                null,
                null,
                0, // Any retries would probably make test failures more confusing
                null,
                null,
                null,
                null,
                DEFAULT_STALEMILLIS,
                0,
                0,
                DEFAULT_MINIMUM_MTIMEMILLIS);
    }

    /**
     * Test configuration for use in backup service tests.
     */
    public static HBackupConfig forTests(String from, String to, int backupIntervalMinutes,
                                         int staleCheckIntervalMinutes) {
        Configuration hdfsConf = new Configuration();
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
                hdfsConf,
                hdfsConf,
                true,
                null,
                null,
                0, // Any retries would probably make test failures more confusing
                null,
                null,
                null,
                null,
                DEFAULT_STALEMILLIS,
                backupIntervalMinutes,
                staleCheckIntervalMinutes,
                DEFAULT_MINIMUM_MTIMEMILLIS);
    }
    
    /**
     * Get config with defaults for all params except those specified. For testing only.
     */
    public static HBackupConfig forTests(String fromUri, String toUri, String hashUri, 
            Configuration hdfsConf, String s3AccessKey, String s3Secret) {
        return new HBackupConfig(fromUri,
                toUri,
                2,
                true,
                s3AccessKey,
                s3Secret,
                s3AccessKey,
                s3Secret,
                MultipartUtils.MIN_PART_SIZE,
                MultipartUtils.MIN_PART_SIZE,
                hdfsConf,
                hdfsConf,
                true,
                null,
                hashUri,
                1,
                s3AccessKey,
                s3Secret,
                null,
                null,
                0,
                0,
                0,
                DEFAULT_MINIMUM_MTIMEMILLIS);
    }
    
    /**
     * Get config with defaults for all params except those specified. For testing only.
     */
    public static HBackupConfig forTests(String fromUri, String toUri, String hashUri, 
            Configuration hdfsSrcConf, Configuration hdfsSinkConf, String s3AccessKey, 
            String s3Secret) {
        return new HBackupConfig(fromUri,
                toUri,
                2,
                true,
                s3AccessKey,
                s3Secret,
                null,
                null,
                MultipartUtils.MIN_PART_SIZE,
                MultipartUtils.MIN_PART_SIZE,
                hdfsSrcConf,
                hdfsSinkConf,
                true,
                null,
                hashUri,
                1,
                s3AccessKey,
                s3Secret,
                null,
                null,
                0,
                0,
                0,
                DEFAULT_MINIMUM_MTIMEMILLIS);
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

       return new HBackupConfig(conf.getString(CONF_FROM, null),
                conf.getString(CONF_TO, null),
                conf.getInt(CONF_CONCURRENTCHUNKS, DEFAULT_CONCURRENT_FILES),
                conf.getBoolean(CONF_RECURSIVE, DEFAULT_RECURSIVE),
                conf.getString(CONF_SOURCES3ACCESSKEY, null),
                conf.getString(CONF_SOURCES3SECRET, null),
                conf.getString(CONF_SINKS3ACCESSKEY, null),
                conf.getString(CONF_SINKS3SECRET, null),
                conf.getLong(CONF_S3PARTSIZE, DEFAULT_S3_PART_SIZE),
                conf.getLong(CONF_S3MULTIPARTTHRESHOLD, DEFAULT_S3_MULTIPART_THRESHOLD),
                new Configuration(true),
                new Configuration(true),
                conf.getBoolean(CONF_MTIMECHECK, DEFAULT_MTIMECHECK),
                conf.getString(CONF_INCLUDEPATHSREGEX, null),
                conf.getString(CONF_CHECKSUMURI, null),
                conf.getInt(CONF_CHUNKRETRIES, DEFAULT_CHUNKRETRIES),
                conf.getString(CONF_CHECKSUMS3ACCESSKEY, null),
                conf.getString(CONF_CHECKSUMS3SECRET, null),
                conf.getString(CONF_FALLBACKS3ACCESSKEY, null),
                conf.getString(CONF_FALLBACKS3SECRET, null),
                conf.getLong(CONF_STALEMILLIS, DEFAULT_STALEMILLIS),
                conf.getInt(CONF_BACKUPINTERVAL, 0),
                conf.getInt(CONF_STALECHECKINTERVAL, 0),
               conf.getLong(CONF_MINIMUM_MTIMEMILLIS, DEFAULT_MINIMUM_MTIMEMILLIS));

    }
    
    final public static OptHelp[] optHelps = new OptHelp[] {
            new OptHelp(CONF_FROM, "URI of data source, e.g. hdfs:///home/bob, hdfs://reports-master-0:7050/home/bob, s3://mybucket/a/b"),
            new OptHelp(CONF_TO, "URI of data sink"),
            new OptHelp(CONF_CONCURRENTCHUNKS, "Number of file chunks to transfer at a time", Integer.toString(DEFAULT_CONCURRENT_FILES)),
            new OptHelp(CONF_RECURSIVE, "Recursively back up the entire source directory tree", Boolean.toString(DEFAULT_RECURSIVE)),
            new OptHelp(CONF_SOURCES3ACCESSKEY, "When the source is an S3 bucket, use this to set its access key"),
            new OptHelp(CONF_SOURCES3SECRET, "When the source is an S3 bucket, use this to set its secret"),
            new OptHelp(CONF_SINKS3ACCESSKEY, "When the destination is an S3 bucket, use this to set its access key"),
            new OptHelp(CONF_SINKS3SECRET, "When the destination is an S3 bucket, use this to set its secret"),
            new OptHelp(CONF_S3PARTSIZE, "When writing to S3 using the multipart API, what size of parts should the file be split into?", 
                    Long.toString(DEFAULT_S3_PART_SIZE)),
            new OptHelp(CONF_S3MULTIPARTTHRESHOLD, "When writing to S3, use the multipart API for files larger than this", 
                    Long.toString(DEFAULT_S3_MULTIPART_THRESHOLD)),
            new OptHelp(CONF_MTIMECHECK, "If true, re-transfer files when the source and sink mtime or length differs. "
                    + "If false, ignore the mtime and only check the length.", Boolean.toString(DEFAULT_MTIMECHECK)),
            new OptHelp(CONF_INCLUDEPATHSREGEX, "If set, only files matching this regex will be sent. Filenames are relative to the backup directory."),
            new OptHelp(CONF_CHECKSUMURI, "Where file checksums should be stored"),
            new OptHelp(CONF_CHECKSUMS3ACCESSKEY, "If the checksums are stored in a protected S3 bucket, specify the access key"),
            new OptHelp(CONF_CHECKSUMS3SECRET, "If the checksums are stored in a protected S3 bucket, specify the secret"),
            new OptHelp(CONF_FALLBACKS3ACCESSKEY, "Use this for all S3 accesses, if all your S3 usage is done under the same account"),
            new OptHelp(CONF_FALLBACKS3SECRET, "Use this for all S3 accesses, if all your S3 usage is done under the same account"),            
            new OptHelp(CONF_STALEMILLIS, "When checking backed-up files for staleness, a file this much older than the source is \"stale\""),
            new OptHelp(CONF_BACKUPINTERVAL, "Determines how often the backup will be run. If the backup can't complete during the interval" +
                    " it will start immediately after the previous run."),
            new OptHelp(CONF_STALECHECKINTERVAL, "Determines how often the stale check will be run. If the check can't complete during the interval" +
                    " it will start immediately after the previous run."),
            new OptHelp(CONF_MINIMUM_MTIMEMILLIS, "The minimum mtime differential between the current " +
                    "system time and a file to be backed up. A file with a differential greater " +
                    "than or equal to this will be backedup.")
    };
    
    public static class OptHelp {
        final String name;
        final String desc;
        final String def; 
        
        public OptHelp(String optName, String help) {
            this(optName, help, null);
        }
        
        public OptHelp(String optName, String help, String def) {
            this.name = optName;
            this.desc = help;
            this.def = def;
        }
    }
}
