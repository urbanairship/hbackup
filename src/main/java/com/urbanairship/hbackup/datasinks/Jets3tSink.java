package com.urbanairship.hbackup.datasinks;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.urbanairship.hbackup.Constant;
import com.urbanairship.hbackup.HBFile;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.Sink;
import com.urbanairship.hbackup.Stats;

// TODO only get remote listing once, instead of once per file

public class Jets3tSink extends Sink {
    private static final Logger log = LogManager.getLogger(HdfsSink.class);
//    private final URI baseUri;
    private final HBackupConfig conf;
    private final S3Service s3Service;
    private final String bucketName;
    private final String baseName;
    private final Stats stats;
    
    public Jets3tSink(URI uri, HBackupConfig conf, Stats stats) throws IOException, URISyntaxException {
//        this.baseUri = uri;
        this.stats = stats;
        this.conf = conf;
        this.bucketName = uri.getHost();
        
        // The path component of the incoming URI, which we will prefix onto all outgoing files,
        // must not begin with "/", and must end with "/".
        String baseNameTemp = uri.getPath();
        if(baseNameTemp.startsWith("/")) {
            baseNameTemp = baseNameTemp.substring(1);            
        }
        if(!baseNameTemp.endsWith("/")) {
            baseNameTemp = baseNameTemp + "/";
        }
        this.baseName = baseNameTemp;
        
        try {
            s3Service = new RestS3Service(conf.s3SinkCredentials);
        } catch (S3ServiceException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean existsAndUpToDate(HBFile file) throws IOException {
        try {
            String sourceRelativePath = file.getRelativePath();
            assert !sourceRelativePath.startsWith("/");
//            S3Object[] listing = s3Service.listObjects(bucketName, baseName + file.getRelativePath(), null);
            
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

//            String sinkRelativePath = stat.getKey().substring(baseName.length());
//            if(sinkRelativePath.startsWith("/")) {
//                sinkRelativePath = sinkRelativePath.substring(1);
//            }
//        
//            for(S3Object stat: listing) {
//                if(sinkRelativePath.equals(sourceRelativePath)) {
//                    // The file exists in the destination.
//                    
//                    // Re-upload if the file in the destination is a different size than the source.
//        
//                    // Re-upload if the file in the destination is based on a previous version
//                    // of the source.
//                    if(!conf.mtimeCheck) {
//                        log.debug("File considered up to date because length was the same and " +
//                                "mtime checking was disabled: " + sourceRelativePath);
//                        return true;
//                    } 
//                    long destMtime;
//                    Object mtimeObj = stat.getMetadata(Constant.S3_SOURCE_MTIME);
//                    if(mtimeObj == null) {
//                        log.debug("Remote object had no source mtime metadata and mtime " + 
//                                "for file " + sourceRelativePath + ", and mtime " + 
//                                "checking is enabled. Will re-upload.");
//                        return false;
//                    }
//                    if(mtimeObj instanceof String) {
//                        try {
//                            destMtime = Long.valueOf((String)mtimeObj);
//                        } catch (NumberFormatException e) {
//                            log.warn("Remote source mtime metadata couldn't be parsed " +
//                                    "as long, value was " + mtimeObj + ". Will re-upload.");
//                            return false;
//                        }
//                    } else {
//                        log.warn("Remote object metadata should have been a String but " +
//                                "was actually " + mtimeObj + " for file " + 
//                                sourceRelativePath + ". Will re-upload.");
//                        return false;
//                    }
//                                
//                    DateTime targetMTime = new DateTime(destMtime, DateTimeZone.UTC);
//                    DateTime sourceMTime = new DateTime(file.getMTime(), DateTimeZone.UTC);
//                    if(targetMTime.equals(sourceMTime)) {
//                        log.debug("Mtime and length match for file, won't re-upload: " + sourceRelativePath);
//                        return true;
//                    } else {
//                        log.debug("Same length but different mtime for file, will re-upload: " + sourceRelativePath);
//                        return false;
//                    }
//                }
//            }
//            log.debug("No matching remote file existed, will upload: " + sourceRelativePath);
//            return false; // No matching remote file was found in the file listing
        } catch (ServiceException e) {
            if(e.getResponseCode() == 404) {
                log.debug("Sink object not present (404) for " + file.getRelativePath() + ", will upload");
                return false;
            } else {
                throw new IOException(e);
            }
        }
    }

    private static enum MultipartTransferState {BEFORE_INIT, IN_PROGRESS, ERROR, DONE};
    
    /**
     * This is slightly complicated. The complication arises from coordinating multiple threads
     * as they transfer multiple chunks for the same file in parallel. The first thread to start
     * a chunk must initialize the multipart transfer, which happens in beforeChunk(). The last
     * thread to finish a chunk must commit the result, which happens in chunkSuccess(). There
     * is a simple state machine to coordinate the initialization/commit/abort of the multipart
     * transfer, and the state machine's state is stored in the "MultipartTransferState state" var.
     */
    private class ChunkWriter {
        private final HBFile file;
        private final List<Runnable> chunks;

        MultipartUpload mpUpload;
        List<MultipartPart> finishedParts = new ArrayList<MultipartPart>();
        
        private final int numChunks;
        private final String destS3Key;
        
        private MultipartTransferState state = MultipartTransferState.BEFORE_INIT;
        
        public ChunkWriter(HBFile hbFile) {
             this.file = hbFile;
             String relativePath = file.getRelativePath();
             assert !relativePath.startsWith("/");
             assert baseName.endsWith("/");
             destS3Key = baseName + relativePath;
             
             final long inputLen = file.getLength();
             if(inputLen >= conf.s3MultipartThreshold) {
                 numChunks = (int)(inputLen / conf.s3PartSize + 1);
                 chunks = new ArrayList<Runnable>(1);
                 
                 for(int i=0; i<numChunks; i++) {
                     final long startAt = i * conf.s3PartSize;
                     final long objLen = Math.min(conf.s3PartSize, inputLen - startAt);
                     final int partNum = i;
                     
                     chunks.add(new Runnable() {
                        @Override
                        public void run() {
                            InputStream partInputStream = null;
                            try {
                                MultipartUpload mpUpload = beforeChunk();
                                
                                partInputStream = file.getPartialInputStream(startAt, objLen);
                                S3Object s3ObjForPart = new S3Object(destS3Key);
                                s3ObjForPart.setDataInputStream(partInputStream);
                                MultipartPart thisPart = s3Service.multipartUploadPart(mpUpload, partNum+1, 
                                        s3ObjForPart);
                                assert thisPart.getSize() == objLen;
                                
                                chunkSuccess(thisPart);
                            } catch (Exception e) {
                                chunkError();
                                stats.transferExceptions.add(e);
                            } finally {
                                if(partInputStream != null) {
                                    try {
                                        partInputStream.close();
                                    } catch (IOException e) { }
                                }
                            }
                        }
                     });
                 }
             } else {
                 numChunks = 1;
                 chunks = new ArrayList<Runnable>(1);
                 chunks.add(new Runnable() {
                    @Override
                    public void run() {
                        InputStream is = null;
                        try {
                            log.debug("Starting regular non-multipart S3 upload of " + file.getRelativePath());
                            S3Object s3Obj = new S3Object(destS3Key);
                            s3Obj.addMetadata(Constant.S3_SOURCE_MTIME, Long.toString(file.getMTime()));
                            is = file.getFullInputStream();
                            s3Obj.setDataInputStream(is);
                            s3Obj.setContentLength(file.getLength());
                            s3Service.putObject(bucketName, s3Obj);
                            log.debug("Finished regular non-multipart S3 upload of " + file.getRelativePath());
                            stats.numChunksSucceeded.incrementAndGet();
                            stats.numFilesSucceeded.incrementAndGet();
                        } catch (Exception e) {
                            stats.transferExceptions.add(e);
                            stats.numFilesFailed.incrementAndGet();
                            stats.numChunksFailed.incrementAndGet();
                        } finally {
                            if(is != null) {
                                try {
                                    is.close();
                                } catch (IOException e) { }
                            }
                        }
                    }
                 });
             }
        }
        
        /**
         * Return the chunks (1 or more) that will collectively transfer this file.
         */
        public List<Runnable> getChunks() {
            return chunks;
        }

        /**
         * Initializes the multipart transfer if it hasn't already been initialized.
         * @returns a MultipartUpload if the transfer should go ahead, or null if some other chunk
         * failed for this file and the chunk should be skipped.
         */
        synchronized private MultipartUpload beforeChunk() throws Exception {
            assert state != MultipartTransferState.DONE;
            
            // Some other chunk had an error. Skip transferring the chunk that would be transferred now.
            if (state == MultipartTransferState.ERROR) {
                stats.numChunksSkipped.incrementAndGet();
                return null;
            }
            
            if(state == MultipartTransferState.BEFORE_INIT) {
                S3Object multipartObj = new S3Object(destS3Key);
                
                // Upload th e source file's mtime as S3 metadata. The next time we run a backup,
                // this will tell us whether we should re-upload the file.
                multipartObj.addMetadata(Constant.S3_SOURCE_MTIME, Long.toString(file.getMTime()));
                log.debug("Starting multipart upload for " + file.getRelativePath());
                mpUpload = s3Service.multipartStartUpload(bucketName, multipartObj);
                state = MultipartTransferState.IN_PROGRESS;
            }
            
            return mpUpload;
        }
        
        /**
         * Finishes and commits the multipart transfer when the last chunk finishes.
         */
        synchronized private void chunkSuccess(MultipartPart part) throws Exception {
            assert state == MultipartTransferState.IN_PROGRESS;
            
            log.debug("Multipart chunk succeeded for " + file.getRelativePath());
            finishedParts.add(part);
            stats.numChunksSucceeded.incrementAndGet();
            if(finishedParts.size() == numChunks) {
                log.debug("Multipart upload complete for " + file.getRelativePath());
                s3Service.multipartCompleteUpload(mpUpload, finishedParts);
                
                state = MultipartTransferState.DONE;
                stats.numFilesSucceeded.incrementAndGet();
            }
        }
        
        /**
         * Called when some chunk couldn't be transferred.
         */
        synchronized private void chunkError() {
            assert state == MultipartTransferState.IN_PROGRESS || state == MultipartTransferState.ERROR;

            if(state == MultipartTransferState.IN_PROGRESS) {
                state = MultipartTransferState.ERROR;
                stats.numFilesFailed.incrementAndGet();
                log.debug("Aborting multipart upload of " + file.getRelativePath() + " due to error");
                try {
                    s3Service.multipartAbortUpload(mpUpload);
                } catch (Exception e) {
                    log.error("Another error when trying to abort multipart upload", e);
                }
            }
            stats.numChunksFailed.incrementAndGet();
        }
    }
    
    @Override
    public List<Runnable> getChunks(HBFile file) {
        return new ChunkWriter(file).getChunks();
    }
}
