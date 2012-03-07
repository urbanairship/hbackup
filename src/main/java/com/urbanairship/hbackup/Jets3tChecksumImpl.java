package com.urbanairship.hbackup;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;

public class Jets3tChecksumImpl extends ChecksumService {
    private static final Logger log = LogManager.getLogger(Jets3tChecksumImpl.class);
    
    private final String bucket;
    private final String baseName;
    private S3Service s3Service;
    
    public Jets3tChecksumImpl(URI uri, HBackupConfig conf) throws IOException {
        this.bucket = uri.getHost();
        this.baseName = Util.canonicalizeBaseName(uri.getPath());
        try {
            this.s3Service = new RestS3Service(conf.s3ChecksumCredentials);
        } catch (ServiceException e) {
            throw new IOException(e);
        }
    }
    
    @Override
    public void storeChecksum(String relativePath, String hexChecksum) throws IOException {
        S3Object s3Object;
        try {
            s3Object = new S3Object(baseName + relativePath, hexChecksum.getBytes());
            s3Service.putObject(bucket, s3Object);
            log.debug("Saved S3 checksum " + hexChecksum + " for " + relativePath);
        } catch (S3ServiceException e) {
            log.error("Couldn't save checksum for " + relativePath, e);
            throw new IOException(e);
        } catch(NoSuchAlgorithmException e) {
            throw new RuntimeException(e); // Will happen if JVM doesn't have MD5, i.e. never
        }
    }

    /**
     * @return the stored checksum for the given relativePath, or null if none was found
     */
    @Override
    public String getChecksum(String relativePath) throws IOException {
        S3Object s3Object = null;
        try {
            s3Object = s3Service.getObject(bucket, baseName + relativePath);
            InputStream is = s3Object.getDataInputStream();
            StringWriter stringWriter = new StringWriter();
            IOUtils.copy(is, stringWriter);
            return stringWriter.toString();
        } catch (ServiceException e) {
            if(e.getResponseCode() == 404) {
                return null;
            } else {
                throw new IOException(e);
            }
        } finally {
            if(s3Object != null) {
                s3Object.closeDataInputStream();
            }
        }
    }

}
