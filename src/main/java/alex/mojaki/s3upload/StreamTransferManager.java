package alex.mojaki.s3upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Manages streaming of data to S3 without knowing the size beforehand and without keeping it all in memory or
 * writing to disk.
 * <p/>
 * The data is split into chunks and uploaded using the multipart upload API
 * without requiring you to know any details in most cases. The uploading is done on separate threads, the number of
 * which is configured by the user.
 * <p/>
 * After creating an instance with details of the upload, use {@link StreamTransferManager#getMultiPartOutputStreams()}
 * to get a list
 * of {@link MultiPartOutputStream}s, one per upload thread. As you write data to these streams, call
 * {@link MultiPartOutputStream#checkSize()} regularly. When you finish, call {@link MultiPartOutputStream#close()}.
 * Parts will be uploaded to S3 as you write.
 * <p/>
 * Once all streams have been closed, call {@link StreamTransferManager#complete()}. Alternatively you can call
 * {@link StreamTransferManager#abort}
 * at any point if needed.
 * <p/>
 * The final file on S3 will then usually be the result of concatenating all the data written to each stream,
 * in the order that the streams were in in the list obtained from {@code getMultiPartOutputStreams()}. However this
 * may not be true if multiple streams are used and some of them produce < 5 MB of data. This is because the multipart
 * upload API does not allow the uploading of more than one part smaller than 5 MB, which leads to fundamental limits
 * on what this class can accomplish. If order of data is important to you, then either use only one stream or ensure
 * that you write at least 5 MB to every stream.
 * <p/>
 * While performing the multipart upload this class will create instances of {@link InitiateMultipartUploadRequest},
 * {@link UploadPartRequest}, and {@link CompleteMultipartUploadRequest}, fill in the essential details, and send them
 * off. If you need to add additional details then override the appropriate {@code customise*Request} methods and
 * set the required properties within.
 * <p/>
 * This class does not perform retries when uploading. If an exception is thrown at any stage the upload will be aborted and the
 * exception rethrown, wrapped in a {@code RuntimeException}.
 *
 * @author Alex Hall
 */
public class StreamTransferManager {

    private static final Logger log = LoggerFactory.getLogger(StreamTransferManager.class);

    protected final String bucketName;
    protected final String putKey;
    protected final AmazonS3 s3Client;
    protected final String uploadId;
    private final List<PartETag> partETags;
    private final List<MultiPartOutputStream> multiPartOutputStreams;
    private final ExecutorServiceResultsHandler<Void> executorServiceResultsHandler;
    private StreamPart leftoverStreamPart = null;
    private final Object leftoverStreamPartLock = new Object();
    private boolean isAborting = false;
    private static final int MAX_PART_NUMBER = 10000;

    /**
     * Initiates a multipart upload to S3 using the first three parameters. Creates
     * {@code numStream MultiPartOutputStream}s and a thread for each one to perform the upload in parallel.
     */
    public StreamTransferManager(String bucketName,
                                 String putKey,
                                 AmazonS3 s3Client,
                                 int numStreams) {
        this.bucketName = bucketName;
        this.putKey = putKey;
        this.s3Client = s3Client;

        log.info("Initiating multipart upload to {}/{}", bucketName, putKey);
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, putKey);
        customiseInitiateRequest(initRequest);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        uploadId = initResponse.getUploadId();
        log.info("Initiated multipart upload to {}/{} with full ID {}", bucketName, putKey, uploadId);
        try {
            partETags = new ArrayList<PartETag>();
            multiPartOutputStreams = new ArrayList<MultiPartOutputStream>();
            ExecutorService threadPool = Executors.newFixedThreadPool(numStreams);
            executorServiceResultsHandler = new ExecutorServiceResultsHandler<Void>(threadPool);

            for (int i = 0; i < numStreams; i++) {
                int partNumberStart = i * MAX_PART_NUMBER / numStreams + 1;
                int partNumberEnd = (i + 1) * MAX_PART_NUMBER / numStreams + 1;
                MultiPartOutputStream multiPartOutputStream = new MultiPartOutputStream(partNumberStart, partNumberEnd);
                multiPartOutputStreams.add(multiPartOutputStream);
                UploadTask uploadTask = new UploadTask(multiPartOutputStream);
                executorServiceResultsHandler.submit(uploadTask);
            }

            executorServiceResultsHandler.finishedSubmitting();
        } catch (Throwable e) {
            abort(e);
            throw new RuntimeException("Unexpected error occurred while setting up streams and threads for upload: this likely indicates a bug in this class.", e);
        }

    }

    public List<MultiPartOutputStream> getMultiPartOutputStreams() {
        return multiPartOutputStreams;
    }

    /**
     * Blocks while waiting for the threads uploading the contents of the streams returned
     * by {@link StreamTransferManager#getMultiPartOutputStreams()} to finish, then sends a request to S3 to complete
     * the upload. For the first part to complete, it's essential that every stream is closed, otherwise the upload
     * threads will block forever waiting for more data.
     */
    public void complete() {
        try {
            log.info("{}:\nWaiting for pool termination", this);
            executorServiceResultsHandler.awaitCompletion();
            log.info("{}:\nPool terminated", this);
            if (leftoverStreamPart != null) {
                log.info("{}:\nUploading leftover stream {}", leftoverStreamPart);
                uploadStreamPart(leftoverStreamPart);
                log.info("{}:\nLeftover uploaded", this);
            }
            log.info("{}:\nCompleting", this);
            CompleteMultipartUploadRequest completeRequest = new
                    CompleteMultipartUploadRequest(
                    bucketName,
                    putKey,
                    uploadId,
                    partETags);
            customiseCompleteRequest(completeRequest);
            s3Client.completeMultipartUpload(completeRequest);
            log.info("{}:\nCompleted", this);
        } catch (Throwable e) {
            abort(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Aborts the upload and logs a message including the stack trace of the given throwable.
     */
    public void abort(Throwable throwable) {
        log.error("{}:\nAbort called due to error:", this, throwable);
        abort();
    }

    /**
     * Aborts the upload. Repeated calls have no effect.
     */
    public void abort() {
        synchronized (this) {
            if (isAborting) {
                return;
            }
            isAborting = true;
        }
        executorServiceResultsHandler.abort();
        log.info("{}:\nAborting", this);
        AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest(
                bucketName, putKey, uploadId);
        s3Client.abortMultipartUpload(abortMultipartUploadRequest);
        log.info("{}:\nAborted", this);
    }

    private class UploadTask implements Callable<Void> {

        private MultiPartOutputStream multiPartOutputStream;

        private UploadTask(MultiPartOutputStream multiPartOutputStream) {
            this.multiPartOutputStream = multiPartOutputStream;
        }

        @Override
        public Void call() {
            try {
                for (StreamPart part : multiPartOutputStream) {
                    if (part.size() < MultiPartOutputStream.S3_MIN_PART_SIZE) {
                        /*
                        Each stream does its best to avoid producing parts smaller than 5 MB, but if a user doesn't
                        write that much data there's nothing that can be done. These are considered 'leftover' parts,
                        and must be merged with other leftovers to try producing a part bigger than 5 MB which can be
                        uploaded without problems. After the threads have completed there may be at most one leftover
                        part remaining, which S3 can accept. It is uploaded in the complete() method.
                         */
                        log.info("{}:\nReceived part {} < 5 MB that needs to be handled as 'leftover'", this, part);
                        StreamPart originalPart = part;
                        part = null;
                        synchronized (leftoverStreamPartLock) {
                            if (leftoverStreamPart == null) {
                                leftoverStreamPart = originalPart;
                                log.info("{}:\nCreated new leftover part {}", this, leftoverStreamPart);
                            } else {
                                /*
                                Try to preserve order within the data by appending the part with the higher number
                                to the part with the lower number. This is not meant to produce a perfect solution:
                                if the client is producing multiple leftover parts all bets are off on order.
                                */
                                if (leftoverStreamPart.getPartNumber() > originalPart.getPartNumber()) {
                                    StreamPart temp = originalPart;
                                    originalPart = leftoverStreamPart;
                                    leftoverStreamPart = temp;
                                }
                                leftoverStreamPart.getOutputStream().append(originalPart.getOutputStream());
                                log.info("{}:\nMerged with existing leftover part to create {}", this, leftoverStreamPart);
                                if (leftoverStreamPart.size() >= MultiPartOutputStream.S3_MIN_PART_SIZE) {
                                    log.info("{}:\nLeftover part can now be uploaded as normal and reset", this);
                                    part = leftoverStreamPart;
                                    leftoverStreamPart = null;
                                }
                            }
                        }
                    }
                    if (part != null) {
                        uploadStreamPart(part);
                    }
                }
                log.info("{}:\nFinished uploading all parts of {}", this, multiPartOutputStream);
            } catch (Throwable t) {
                abort(t);
                throw new RuntimeException(t);
            }
            return null;
        }
    }

    private void uploadStreamPart(StreamPart part) {
        log.info("{}:\nUploading {}", this, part);

        UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(bucketName).withKey(putKey)
                .withUploadId(uploadId).withPartNumber(part.getPartNumber())
                .withInputStream(part.getInputStream())
                .withPartSize(part.size());
        customiseUploadPartRequest(uploadRequest);

        UploadPartResult uploadPartResult = s3Client.uploadPart(uploadRequest);
        PartETag partETag = uploadPartResult.getPartETag();
        partETags.add(partETag);
        log.info("{}:\nFinished uploading {}", this, part);
    }

    @Override
    public String toString() {
        return String.format("[Manager uploading to %s/%s with id %s]",
                bucketName, putKey, Utils.skipMiddle(uploadId, 21));
    }

    // These methods are intended to be overridden for more specific interactions with the AWS API.

    public void customiseInitiateRequest(InitiateMultipartUploadRequest request) {
    }

    public void customiseUploadPartRequest(UploadPartRequest request) {
    }

    public void customiseCompleteRequest(CompleteMultipartUploadRequest request) {
    }

}