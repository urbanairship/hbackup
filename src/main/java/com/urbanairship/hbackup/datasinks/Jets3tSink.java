package com.urbanairship.hbackup.datasinks;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.MultipartCompleted;
import org.jets3t.service.model.MultipartPart;
import org.jets3t.service.model.MultipartUpload;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.urbanairship.hbackup.HBFile;
import com.urbanairship.hbackup.HBackupConfig;
import com.urbanairship.hbackup.Sink;

public class Jets3tSink extends Sink {
//    private static final Logger log = LogManager.getLogger(HdfsSink.class);
//    private final URI baseUri;
    private final HBackupConfig conf;
//    private final AWSCredentials awsCreds;
    private final S3Service s3ServiceWithLiveMd5;
    private final S3Service s3ServiceNoLiveMd5; // jets3t bug workaround: no hash verification of multipart uploads
    private final String bucketName;
    private final String baseName;
    
    public Jets3tSink(URI uri, HBackupConfig conf) throws IOException, URISyntaxException {
//        this.baseUri = uri;
        this.conf = conf;
        this.bucketName = uri.getHost();
        String uriPath = uri.getPath();
        if(uriPath.startsWith("/")) {
            this.baseName = uriPath.substring(1);            
        } else {
            this.baseName = uriPath;
        }
        
//        AWSCredentials awsCreds = new AWSCredentials(conf.sinkS3AccessKey, conf.sinkS3Secret);
        try {
            s3ServiceWithLiveMd5 = new RestS3Service(conf.s3SinkCredentials);
            
            // We have to turn off hash checking on multipart uploads due to jets3t bug #141
            s3ServiceNoLiveMd5 = new RestS3Service(conf.s3SinkCredentials);
            s3ServiceNoLiveMd5.getJetS3tProperties().setProperty("storage-service.disable-live-md5", "true");
        } catch (S3ServiceException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean existsAndUpToDate(HBFile file) throws IOException {
        try {
            String sourceRelativePath = file.getRelativePath();
            S3Object[] listing = s3ServiceWithLiveMd5.listObjects(bucketName, baseName + file.getRelativePath(), null);
            for(S3Object stat: listing) {
                String sinkRelativePath = stat.getKey().substring(baseName.length()+1);
                if(sinkRelativePath.equals(sourceRelativePath)) {
                    DateTime targetMTime = new DateTime(stat.getLastModifiedDate(), DateTimeZone.UTC);
                    DateTime sourceMTime = new DateTime(file.getMTime(), DateTimeZone.UTC);
                    if(targetMTime.isBefore(sourceMTime)) {
                        return false; // Remote file is older, we should overwrite it
                    } else {
                        return true; // Remote file is newer, we should leave it along
                    }
                }
            }
            return false; // No matching remote file was found
        } catch (ServiceException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void write(HBFile file) throws IOException {
        // TODO short-circuit copy for s3-to-s3 backup?
        // Use multipart upload for large files, otherwise regular upload
        long inputLen = file.getLength();
        String destKey = baseName + file.getRelativePath();
        try {
            if(inputLen >= conf.s3MultipartThreshold) {
                int numParts = (int)(inputLen / conf.s3PartSize + 1);
                List<MultipartPart> parts = new ArrayList<MultipartPart>(numParts);
                MultipartUpload mpUpload = s3ServiceNoLiveMd5.multipartStartUpload(bucketName, 
                        new S3Object(destKey));
                long totalPartBytesUploaded = 0;
                for(int i=0; i<numParts; i++) {
                    long startAt = i * conf.s3PartSize;
                    long objLen = Math.min(conf.s3PartSize, inputLen - startAt);
                    InputStream partInputStream = file.getPartialInputStream(startAt, objLen);
                    S3Object s3ObjForPart = new S3Object(destKey);
                    s3ObjForPart.setDataInputStream(partInputStream);
//                    s3ObjForPart.setContentLength(objLen);
                    System.err.println("********* Uploading part " + i + " with size " + objLen);
                    MultipartPart thisPart = s3ServiceNoLiveMd5.multipartUploadPart(mpUpload, i+1, 
                            s3ObjForPart);
                    System.err.println("********* Uploaded part " + i + " had size " + thisPart.getSize() + 
                            " and expected size " + objLen);
                    assert thisPart.getSize() == objLen;
                    parts.add(thisPart);
                    totalPartBytesUploaded += objLen;
                }
                assert totalPartBytesUploaded == inputLen;
                MultipartCompleted mc = s3ServiceNoLiveMd5.multipartCompleteUpload(mpUpload, parts);
                
            } else {
                S3Object s3Obj = new S3Object(destKey);
                s3Obj.setDataInputStream(file.getFullInputStream());
                s3Obj.setContentLength(file.getLength());
                s3ServiceWithLiveMd5.putObject(bucketName, s3Obj);
            }
        } catch (S3ServiceException e) {
            throw new IOException(e);
        }
    }
}
