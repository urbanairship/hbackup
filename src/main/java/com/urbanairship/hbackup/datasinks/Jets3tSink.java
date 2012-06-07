/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.hbackup.datasinks;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.MultipartPart;
import org.jets3t.service.model.MultipartUpload;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;

import com.urbanairship.hbackup.ChecksumService;
import com.urbanairship.hbackup.Constant;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.RetryableChunk;
import com.urbanairship.hbackup.Sink;
import com.urbanairship.hbackup.SourceFile;
import com.urbanairship.hbackup.Stats;
import com.urbanairship.hbackup.StreamingXor;
import com.urbanairship.hbackup.Util;
import com.urbanairship.hbackup.XorInputStream;

// TODO only get remote listing once, instead of once per file

public class Jets3tSink extends Sink {
    private static final Logger log = LogManager.getLogger(HdfsSink.class);
    private final HBackupConfig conf;
    private final S3Service s3Service;
    private final String bucketName;
    private final String baseName;
    
    public Jets3tSink(URI uri, HBackupConfig conf, Stats stats, ChecksumService checksumService)  throws IOException, URISyntaxException {
        this.conf = conf;
        this.bucketName = uri.getHost();
        
        // The path component of the incoming URI, which we will prefix onto all outgoing files,
        // must not begin with "/", and must end with "/".
        this.baseName = Util.canonicalizeBaseName(uri.getPath());
        
        try {
            s3Service = new RestS3Service(conf.s3SinkCredentials);
        } catch (S3ServiceException e) {
            throw new IOException(e);
        }
    }
    
    @Override
    public boolean existsAndUpToDate(SourceFile file) throws IOException {
        try {
            String sourceRelativePath = file.getRelativePath();
            assert !sourceRelativePath.startsWith("/");
            
            StorageObject s3Obj = s3Service.getObjectDetails(bucketName, baseName + file.getRelativePath());
            if(s3Obj == null) {
                log.debug("No matching remote file existed, will upload: " + sourceRelativePath);
                return false; // No matching remote file was found in the file listing
            }
            if(file.getLength() != s3Obj.getContentLength()) {
                log.debug("File in destination had a different length than the source " +
                        " for " + sourceRelativePath + ". Will re-upload.");
                return false;
            }
            if(!conf.mtimeCheck) {
                log.debug("Mtime checking was disabled and filesize matched. Won't reupload " + sourceRelativePath);
                return true;
            }
            
            Object mtimeObj = s3Obj.getMetadata(Constant.S3_SOURCE_MTIME);
            if(mtimeObj == null) {
                log.debug("Remote object had no source mtime metadata and mtime " + 
                        "for file " + sourceRelativePath + ", and mtime " + 
                        "checking is enabled. Will re-upload.");
                return false;
            }
            if(mtimeObj instanceof String) {
                try {
                    long destMtime = Long.valueOf((String)mtimeObj);
                    
                    if(file.getMTime() == destMtime) {
                        log.debug("Mtime and length match for file, won't re-upload: " + sourceRelativePath);
                        return true;
                    } else {
                        log.debug("Same length but different mtime for file, will re-upload: " + sourceRelativePath);
                        return false;
                    }
                } catch (NumberFormatException e) {
                    log.warn("Remote source mtime metadata couldn't be parsed " +
                            "as long, value was " + mtimeObj + ". Will re-upload.");
                    return false;
                }
            } else {
                log.warn("Remote object metadata should have been a String but " +
                        "was actually " + mtimeObj + " for file " + 
                        sourceRelativePath + ". Will re-upload.");
                return false;
            }
        } catch (ServiceException e) {
            if(e.getResponseCode() == 404) {
                log.debug("Sink object not present (404) for " + file.getRelativePath() + ", will upload");
                return false;
            } else {
                throw new IOException(e);
            }
        }
    }

    @Override
    public Long getMTime(String relativePath) throws IOException {
        try {
            StorageObject s3Obj = s3Service.getObjectDetails(bucketName, baseName + relativePath);
            
            // If the object was backed up to S3 by hbackup, it may have a our special app-specific
            // metadata attached giving the mtime of the source file. We'll use this as the mtime
            // for staleness checking purposes.
            
            Object mtimeObj = s3Obj.getMetadataMap().get(Constant.S3_SOURCE_MTIME);
            if(mtimeObj != null) {
                if(mtimeObj instanceof String) {
                    try {
                        return Long.valueOf((String)mtimeObj);
                    } catch (NumberFormatException e) { }
                } 
                final String msg = "Malformed S3 source mtime metadata for " + relativePath; 
                log.error(msg);
                throw new IOException(msg);
            } else {
                // Special hbackup source mtime metadata wasn't present. Fall back on S3's built in 
                // last-modified timestamp.
                return s3Obj.getLastModifiedDate().getTime();
            }
        } catch (ServiceException e) {
            if(e.getResponseCode() == 404) {
                return null;
            } else {
                // It was a serious exception and not just a 404. Re-throw.
                throw new IOException(e);
            }
        }
    }
    
//    private static enum MultipartTransferState {BEFORE_INIT, IN_PROGRESS, ERROR, DONE};
    
    /**
     * This is slightly complicated. The complication arises from coordinating multiple threads
     * as they transfer multiple chunks for the same file in parallel. The first thread to start
     * a chunk must initialize the multipart transfer, which happens in beforeChunk(). The last
     * thread to finish a chunk must commit the result, which happens in chunkSuccess(). There
     * is a simple state machine to coordinate the initialization/commit/abort of the multipart
     * transfer, and the state machine's state is stored in the "MultipartTransferState state" var.
     */
    private class ChunkWriter {
        private final SourceFile file;
        private final List<RetryableChunk> chunks;
        private final List<MultipartPart> finishedParts = Collections.synchronizedList(new ArrayList<MultipartPart>());
        private final int numChunks;
        private final String destS3Key;
        private final String relativePath;
        
        private final Object multiPartInitLock = new Object();
        private MultipartUpload mpUpload = null;
        
        public ChunkWriter(SourceFile hbFile) {
             this.file = hbFile;
             relativePath = file.getRelativePath();
             assert !relativePath.startsWith("/");
             destS3Key = baseName + relativePath;
             
             final long inputLen = file.getLength();
             if(inputLen >= conf.s3MultipartThreshold) {
                 numChunks = (int)(inputLen / conf.s3PartSize + 1);
                 chunks = new ArrayList<RetryableChunk>(1);
                 
                 for(int i=0; i<numChunks; i++) {
                     final long startAt = i * conf.s3PartSize;
                     final long objLen = Math.min(conf.s3PartSize, inputLen - startAt);
                     final int partNum = i;
                     
                     chunks.add(new RetryableChunk() {
                        @Override
                        public StreamingXor run() throws IOException {
                            InputStream partInputStream = null;
                            try {
                                synchronized (multiPartInitLock) {
                                    // Initialize the multipart upload if not already done.
                                    if(mpUpload == null) {
                                        S3Object multipartObj = new S3Object(destS3Key);
                                        // Upload the source file's mtime as S3 metadata. The next time we run a backup,
                                        // this will tell us whether we should re-upload the file.
                                        multipartObj.addMetadata(Constant.S3_SOURCE_MTIME, Long.toString(file.getMTime()));
                                        log.debug("Starting multipart upload for " + relativePath);
                                        mpUpload = s3Service.multipartStartUpload(bucketName, multipartObj);
                                    }
                                }
                                
                                partInputStream = file.getPartialInputStream(startAt, objLen);
                                XorInputStream xis = new XorInputStream(partInputStream, startAt);
                                S3Object s3ObjForPart = new S3Object(destS3Key);
                                s3ObjForPart.setDataInputStream(xis);
                                MultipartPart thisPart = s3Service.multipartUploadPart(mpUpload, partNum+1, 
                                        s3ObjForPart);
                                assert thisPart.getSize() == objLen;
                                finishedParts.add(thisPart);
                                return xis.getStreamingXor();
                            } catch (S3ServiceException e) {
                                throw new IOException(e);
                            } finally {
                                if(partInputStream != null) {
                                    try {
                                        partInputStream.close();
                                    } catch (IOException e) { }
                                }
                            }
                        }

                        @Override
                        public void commitAllChunks() throws IOException {
                            try {
                                log.info("Multipart upload complete for " + relativePath);
                                s3Service.multipartCompleteUpload(mpUpload, finishedParts);
                            } catch (S3ServiceException e) {
                                throw new IOException(e);
                            }
                        }
                     });
                 }
             } else {
                 numChunks = 1;
                 chunks = new ArrayList<RetryableChunk>(1);
                 chunks.add(new RetryableChunk() {
                    @Override
                    public StreamingXor run() throws IOException {
                        InputStream sourceStream = null;
                        try {
                            log.debug("Starting regular non-multipart S3 upload of " + relativePath);
                            S3Object s3Obj = new S3Object(destS3Key);
                            s3Obj.addMetadata(Constant.S3_SOURCE_MTIME, Long.toString(file.getMTime()));
                            sourceStream = file.getFullInputStream();
                            XorInputStream xis = new XorInputStream(sourceStream, 0);
                            s3Obj.setDataInputStream(xis);
                            s3Service.putObject(bucketName, s3Obj);
                            log.debug("Finished regular non-multipart S3 upload of " + relativePath);
                            return xis.getStreamingXor();
                        } catch (ServiceException e) {
                            throw new IOException(e);
                        } finally {
                            if(sourceStream != null) {
                                try {
                                    sourceStream.close();
                                } catch (IOException e) { }
                            }
                        }
                    }

                    @Override
                    public void commitAllChunks() throws IOException {
                        log.debug("Commit noop, nothing to do for simple S3 uploads");
                    }
                 });
             }
        }
        
        /**
         * Return the chunks (1 or more) that will collectively transfer this file.
         */
        public List<RetryableChunk> getChunks() {
            return chunks;
        }
    }
    
    @Override
    public List<RetryableChunk> getChunks(SourceFile file) {
        return new ChunkWriter(file).getChunks();
    }
}
