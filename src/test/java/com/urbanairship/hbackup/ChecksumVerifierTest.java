package com.urbanairship.hbackup;
import org.apache.hadoop.conf.Configuration;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.utils.MultipartUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import checksumverify.ChecksumStats;
import checksumverify.ChecksumVerify;

public class ChecksumVerifierTest extends S3SetupAndTeardownTest{

    @BeforeClass
    public static void beforeClass() throws Exception {
        S3SetupAndTeardownTest.beforeClass();
    }
    
    @AfterClass
    public static void afterClass() {
        S3SetupAndTeardownTest.afterClass();
    }
    
    @Before
    public void before() throws Exception {
        super.beforeEach();
    }
    
    @After
    public void after() throws Exception {
        super.afterEach();
    }
    
    /**
     * Write two files and two checksums into S3, where one checksum doesn't match. Assert that
     * the corrrect and incorrect checksums are both detected.
     */
    @Test
    public void oneOfEach() throws Exception {
        // Write an object with a correct checksum
        byte[] buf1 = TestUtil.getRandomBuf(128);
        sinkService.putObject(sourceBucket, new S3Object("files/file_with_correct_checksum", buf1));
        sinkService.putObject(sourceBucket, new S3Object("hashes/file_with_correct_checksum",
                TestUtil.expectedXor(buf1)));
        
        // Write an object with an incorrect checksum
        byte[] buf2 = TestUtil.getRandomBuf(50);
        sinkService.putObject(sourceBucket, new S3Object("files/file_with_bad_checksum", buf2));
        sinkService.putObject(sourceBucket, new S3Object("hashes/file_with_bad_checksum", "1234567"));
        
        HBackupConfig conf = new HBackupConfig("s3://" + sourceBucket + "/files",
                null,
                2,
                true,
                System.getProperty(HBackupConfig.CONF_SOURCES3ACCESSKEY),
                System.getProperty(HBackupConfig.CONF_SOURCES3SECRET),
                null,
                null,
                MultipartUtils.MIN_PART_SIZE,
                MultipartUtils.MIN_PART_SIZE,
                new Configuration(),
                true,
                null,
                "s3://" + sourceBucket + "/hashes",
                1,
                System.getProperty(HBackupConfig.CONF_CHECKSUMS3ACCESSKEY),
                System.getProperty(HBackupConfig.CONF_CHECKSUMS3SECRET));
        ChecksumVerify checksumVerify = new ChecksumVerify(conf);
        Assert.assertEquals(false, checksumVerify.runWithCheckedExceptions());
        
        ChecksumStats stats = checksumVerify.getStats();
        Assert.assertEquals(1, stats.matched.get());
        Assert.assertEquals(1, stats.mismatched.get());
    }
    
    @Test
    public void allMatch() throws Exception {
        byte[] buf1 = TestUtil.getRandomBuf(128);
        byte[] buf2 = TestUtil.getRandomBuf(128);
        
        String file1Key = "files/file1_with_correct_checksum";
        String hash1Key = "hashes/file1_with_correct_checksum";
        String file2Key = "files/file2_with_correct_checksum";
        String hash2Key = "hashes/file2_with_correct_checksum";
        
        deleteLater(sourceService, sourceBucket, file1Key);
        deleteLater(sourceService, sourceBucket, hash1Key);
        deleteLater(sourceService, sourceBucket, file2Key);
        deleteLater(sourceService, sourceBucket, hash2Key);
        
        sinkService.putObject(sourceBucket, new S3Object(file1Key, buf1));
        sinkService.putObject(sourceBucket, new S3Object(hash1Key, TestUtil.expectedXor(buf1)));
        sinkService.putObject(sourceBucket, new S3Object(file2Key, buf2));
        sinkService.putObject(sourceBucket, new S3Object(hash2Key, TestUtil.expectedXor(buf2)));
        
        HBackupConfig conf = new HBackupConfig("s3://" + sourceBucket + "/files",
                null,
                2,
                true,
                System.getProperty(HBackupConfig.CONF_SOURCES3ACCESSKEY),
                System.getProperty(HBackupConfig.CONF_SOURCES3SECRET),
                null,
                null,
                MultipartUtils.MIN_PART_SIZE,
                MultipartUtils.MIN_PART_SIZE,
                new Configuration(),
                true,
                null,
                "s3://" + sourceBucket + "/hashes",
                1,
                System.getProperty(HBackupConfig.CONF_CHECKSUMS3ACCESSKEY),
                System.getProperty(HBackupConfig.CONF_CHECKSUMS3SECRET));
        ChecksumVerify checksumVerify = new ChecksumVerify(conf);
        Assert.assertTrue(checksumVerify.runWithCheckedExceptions());
        ChecksumStats stats = checksumVerify.getStats();
        Assert.assertEquals(2, stats.matched.get());
    }
    
    @Test
    public void missingChecksum() throws Exception {
        byte[] buf1 = TestUtil.getRandomBuf(128);
        
        String file1Key = "files/file1_with_missing_checksum";
        
        deleteLater(sourceService, sourceBucket, file1Key);
        
        sinkService.putObject(sourceBucket, new S3Object(file1Key, buf1));
        
        HBackupConfig conf = new HBackupConfig("s3://" + sourceBucket + "/files",
                null,
                2,
                true,
                System.getProperty(HBackupConfig.CONF_SOURCES3ACCESSKEY),
                System.getProperty(HBackupConfig.CONF_SOURCES3SECRET),
                null,
                null,
                MultipartUtils.MIN_PART_SIZE,
                MultipartUtils.MIN_PART_SIZE,
                new Configuration(),
                true,
                null,
                "s3://" + sourceBucket + "/hashes",
                1,
                System.getProperty(HBackupConfig.CONF_CHECKSUMS3ACCESSKEY),
                System.getProperty(HBackupConfig.CONF_CHECKSUMS3SECRET));
        ChecksumVerify checksumVerify = new ChecksumVerify(conf);
        Assert.assertFalse(checksumVerify.runWithCheckedExceptions());
        ChecksumStats stats = checksumVerify.getStats();
        Assert.assertEquals(1, stats.missingChecksums.get());
    }
}
