package com.urbanairship.hbackup.datasinks;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.MultipartPart;
import org.jets3t.service.model.MultipartUpload;
import org.jets3t.service.model.S3Object;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.urbanairship.hbackup.Constant;
import com.urbanairship.hbackup.HBFile;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.Sink;
import com.urbanairship.hbackup.Stats;

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
        String uriPath = uri.getPath();
        if(uriPath.startsWith("/")) {
            this.baseName = uriPath.substring(1);            
        } else {
            this.baseName = uriPath;
        }
        
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
            S3Object[] listing = s3Service.listObjects(bucketName, baseName + file.getRelativePath(), null);
            for(S3Object stat: listing) {
                String sinkRelativePath = stat.getKey().substring(baseName.length()+1);
                if(sinkRelativePath.equals(sourceRelativePath)) {
                    // The file exists in the destination.
                    
                    // Re-upload if the file in the destination is a different size than the source.
                    if(file.getLength() != stat.getContentLength()) {
                        log.debug("File in destination had a different length than the source " +
                                " for " + sourceRelativePath + ". Will re-upload.");
                        return false;
                    }

                    // Re-upload if the file in the destination is based on a previous version
                    // of the source.
                    if(!conf.mtimeCheck) {
                        return true;
                    } 
                    long destMtime;
                    Object mtimeObj = stat.getMetadata(Constant.S3_SOURCE_MTIME);
                    if(mtimeObj == null) {
                        log.debug("Remote object had no source mtime metadata and mtime " + 
                                "for file " + sourceRelativePath + ", and mtime " + 
                                "checking is enabled. Will re-upload.");
                        return false;
                    }
                    if(mtimeObj instanceof String) {
                        try {
                            destMtime = Long.valueOf((String)mtimeObj);
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
                                
                    DateTime targetMTime = new DateTime(destMtime, DateTimeZone.UTC);
                    DateTime sourceMTime = new DateTime(file.getMTime(), DateTimeZone.UTC);
                    if(targetMTime.equals(sourceMTime)) {
                        return true;
                    } else {
                        return false;
                    }
                }
            }
            return false; // No matching remote file was found in the file listing
        } catch (ServiceException e) {
            throw new IOException(e);
        }
    }

    private static enum MultipartTransferState {BEFORE_INIT, IN_PROGRESS, ERROR, DONE};
    
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
             destS3Key = baseName + file.getRelativePath();
             
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
                            try {
                                MultipartUpload mpUpload = beforeChunk();
                                
                                InputStream partInputStream = file.getPartialInputStream(startAt, objLen);
                                S3Object s3ObjForPart = new S3Object(destS3Key);
                                s3ObjForPart.setDataInputStream(partInputStream);
                                System.err.println("********* Uploading part " + partNum + " with size " + objLen);
                                MultipartPart thisPart = s3Service.multipartUploadPart(mpUpload, partNum+1, 
                                        s3ObjForPart);
                                System.err.println("********* Uploaded part " + partNum + " with size " + thisPart.getSize() + 
                                        " and expected size " + objLen);
                                assert thisPart.getSize() == objLen;
                                
                                chunkSuccess(thisPart);
                            } catch (Exception e) {
                                chunkError();
                                stats.transferExceptions.add(e);
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
                        try {
                            S3Object s3Obj = new S3Object(destS3Key);
                            s3Obj.addMetadata(Constant.S3_SOURCE_MTIME, Long.toString(file.getMTime()));
                            s3Obj.setDataInputStream(file.getFullInputStream());
                            s3Obj.setContentLength(file.getLength());
                            s3Service.putObject(bucketName, s3Obj);
                        } catch (Exception e) {
                            stats.transferExceptions.add(e);
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
            
            finishedParts.add(part);
            stats.numChunksSucceeded.incrementAndGet();
            if(finishedParts.size() == numChunks) {
                s3Service.multipartCompleteUpload(mpUpload, finishedParts);
                
                state = MultipartTransferState.DONE;
                stats.numFilesSucceeded.incrementAndGet();
            }
        }
        
        /**
         * Called when some chunk couldn't be transferred.
         */
        synchronized private void chunkError() {
            assert state == MultipartTransferState.IN_PROGRESS;
            
            state = MultipartTransferState.ERROR;
            try {
                s3Service.multipartAbortUpload(mpUpload);
            } catch (Exception e) {
                log.error("Another error when trying to abort multipart upload", e);
            }
            stats.numChunksFailed.incrementAndGet();
        }
    }
    
    @Override
    public List<Runnable> getChunks(HBFile file) {
        return new ChunkWriter(file).getChunks();
    }
}
