# HBackup: a backup utility for large datasets in HDFS and S3

## Features

HBackup transfers large files between HDFS and S3 and keeps them up to date. Its main features:

 - Can use S3 multipart upload, which:
 - Transfers multiple chunks of the same file in parallel
 - Bypasses the 5GB file limit to allow very large files
 - Overwrites files in the destination if their size or mtime is different from the source file
 - Uses a custom checksum to verify the integrity of backups in S3 (normal S3 checksums don't work for multipart uploads)
 - Provides a health check which can verify that all files in the destination are within some delta of the corresponding source file

Why would I choose this tool instead of Hadoop distcp?

- Distcp requires a mapreduce cluster, and the Hadoop S3FileSystem doesn't
  support uploading files over 5GB since it doesn't use the multipart upload
  API. Depending on your use case, these factors might be important, but distcp
  does fine in many cases.

## Usage

There are three "main" functions in HBackup:

 - The main backup tool, run com.urbanairship.hbackup.HBackup. This will do a
   backup according to the configuration. It will only backup files with an
   age greater than or equal to `hbackup.mtimeAgeMillis`.

 - The S3 in-place checksum verification tool, run
   com.urbanairship.hbackup.checksumverify.ChecksumVerify. This will compute
   checksums of files in S3 and compare the checksums against the their expected
   values, which are stored separately in S3.

 - The backup staleness/health check, run
   com.urbanairship.hbackup.StalenessCheck. This tool will compare the source
   and destination files and exit with a nonzero error code if any destination
   file is older than the corresponding source file by more than a given amount
   of time. This could be used in a nagios health check.

## Configuration

Configuration can be passed in two different ways. The first way is to pass the
name of a properties file as a command line argument (e.g. java
com.urbanairship.hbackup.HBackup myconfig.props). The second way is to set JVM
system properties (e.g. java -Dhbackup.from=hdfs://etc -Dhbackup.to=s3//etc
com.urbanairship.hbackup.HBackup). Configuration values in JVM properties
override the ones in properties files.

## Important configuration values

*hbackup.from*: The source of backup data, either an HDFS URI or S3 URI of the
 form "hdfs://namenode:port/dir" or "s3://bucket/prefix".

*hbackup.to*: The destination for the backup data, either an HDFS URI or S3 URI.

*hbackup.s3AccessKey* and *hbackup.s3Secret*: The security credentials for
 accessing S3. If you need to use different credentials for the source,
 destination, or checksum storage, there are other options you can use (see
 below).

## All configuration values

All options can be seen by running com.urbanairship.hbackup.HBackup with --usage as a command line argument:

    Usage: CLASSPATH=... java -Dprop=val -Dprop=val com.urbanairship.hbackup.HBackup [resource] [resource]
    The "resource"s are filenames or URLs pointing to properties files which may set config values.
    You can set config values in the resource files or by setting JVM system properties with -Dprop=val.
    
    The available config values are:
     hbackup.from                    URI of data source, e.g. hdfs:///home/bob, hdfs://reports-master-0:7050/home/bob, s3://mybucket/a/b
     hbackup.to                      URI of data sink
     hbackup.concurrentChunks        Number of file chunks to transfer at a time (default 5)
     hbackup.recursive               Recursively back up the entire source directory tree (default true)
     hbackup.from.s3AccessKey        When the source is an S3 bucket, use this to set its access key
     hbackup.from.s3Secret           When the source is an S3 bucket, use this to set its secret
     hbackup.to.s3AccessKey          When the destination is an S3 bucket, use this to set its access key
     hbackup.to.s3Secret             When the destination is an S3 bucket, use this to set its secret
     hbackup.s3.partSize             When writing to S3 using the multipart API, what size of parts should the file be split into? (default 104857600)
     hbackup.s3.multipartThreshold   When writing to S3, use the multipart API for files larger than this (default 104857600)
     hbackup.mtimecheck              If true, re-transfer files when the source and sink mtime or length differs. If false, ignore the mtime and only check the length. (default true)
     hbackup.includePathsRegex       If set, only files matching this regex will be sent. Filenames are relative to the backup directory.
     hbackup.checksumUri             Where file checksums should be stored
     hbackup.checksum.s3AccessKey    If the checksums are stored in a protected S3 bucket, specify the access key
     hbackup.checksum.s3Secret       If the checksums are stored in a protected S3 bucket, specify the secret
     hbackup.s3AccessKey             Use this for all S3 accesses, if all your S3 usage is done under the same account
     hbackup.s3Secret                Use this for all S3 accesses, if all your S3 usage is done under the same account
     hbackup.staleMillis             When checking backed-up files for staleness, a file this much older than the source is "stale"
     hbackup.mtimeAgeMillis          When checking source up files for inclusion, a file this much older than the current systemTime will be backed up.
    
    When specifying HDFS URIs, you can leave the host part blank
    (hdfs://dir/file.txt instead of hdfs://host:port/dir/file.txt) if the
    classpath contains a Hadoop configuration pointing to a default filesystem.

    Examples:
      CLASSPATH=hbackup-0.9-jar-with-dependencies.jar java -Dhbackup.from=hdfs:///from -Dhbackup.to=hdfs:///to com.urbanairship.hbackup.HBackup ./otherconfigs.properties


## Example of doing a backup
Assuming you have a properties file named "./s3credentials.properties" that sets hbackup.s3AccessKey and hbackup.s3Secret:

    CLASSPATH=hbackup-0.9-jar-with-dependencies.jar java -Dhbackup.from=hdfs:///from -Dhbackup.to=hdfs:///to com.urbanairship.hbackup.HBackup ./s3credentials.properties

## Example of verifying checksums in S3
Assuming you have a properties file named "./s3credentials.properties" that sets hbackup.s3AccessKey and hbackup.s3Secret:

    CLASSPATH=hbackup-0.9-jar-with-dependencies.jar java -Dhbackup.from=s3://mybucket/files -Dhbackup.checksumUri=s3://mybucket/checksums com.urbanairship.hbackup.checksumverify.ChecksumVerify

## Example of a staleness health check
Assuming you have a properties file named "./s3credentials.properties" that sets hbackup.s3AccessKey and hbackup.s3Secret:

    CLASSPATH=hbackup-0.9-jar-with-dependencies.jar java -Dhbackup.from=s3://mybucket/files -Dhbackup.checksumUri=s3://mybucket/checksums com.urbanairship.hbackup.checksumverify.ChecksumVerify
