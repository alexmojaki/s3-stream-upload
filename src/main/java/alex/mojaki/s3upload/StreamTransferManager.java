package alex.mojaki.s3upload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.BinaryUtils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;


// @formatter:off
/**
 * Manages streaming of data to S3 without knowing the size beforehand and without keeping it all in memory or
 * writing to disk.
 * <p>
 * The data is split into chunks and uploaded using the multipart upload API by one or more separate threads.
 * <p>
 * After creating an instance with details of the upload, use {@link StreamTransferManager#getMultiPartOutputStreams()}
 * to get a list
 * of {@link MultiPartOutputStream}s. When you finish writing data, call {@link MultiPartOutputStream#close()}.
 * Parts will be uploaded to S3 as you write.
 * <p>
 * Once all streams have been closed, call {@link StreamTransferManager#complete()}. Alternatively you can call
 * {@link StreamTransferManager#abort()}
 * at any point if needed.
 * <p>
 * Here is an example. A lot of the code relates to setting up threads for creating data unrelated to the library. The
 * essential parts are commented.
* <pre>{@code
    AmazonS3Client client = new AmazonS3Client(awsCreds);

    // Setting up
    int numStreams = 2;
    final StreamTransferManager manager = new StreamTransferManager(bucket, key, client)
            .numStreams(numStreams)
            .numUploadThreads(2)
            .queueCapacity(2)
            .partSize(10);
    final List<MultiPartOutputStream> streams = manager.getMultiPartOutputStreams();

    ExecutorService pool = Executors.newFixedThreadPool(numStreams);
    for (int i = 0; i < numStreams; i++) {
        final int streamIndex = i;
        pool.submit(new Runnable() {
            public void run() {
                try {
                    MultiPartOutputStream outputStream = streams.get(streamIndex);
                    for (int lineNum = 0; lineNum < 1000000; lineNum++) {
                        String line = generateData(streamIndex, lineNum);

                        // Writing data and potentially sending off a part
                        outputStream.write(line.getBytes());
                    }

                    // The stream must be closed once all the data has been written
                    outputStream.close();
                } catch (Exception e) {

                    // Aborts all uploads
                    manager.abort(e);
                }
            }
        });
    }
    pool.shutdown();
    pool.awaitTermination(5, TimeUnit.SECONDS);

    // Finishing off
    manager.complete();
 * }</pre>
 * <p>
 * The final file on S3 will then usually be the result of concatenating all the data written to each stream,
 * in the order that the streams were in in the list obtained from {@code getMultiPartOutputStreams()}. However this
 * may not be true if multiple streams are used and some of them produce less than 5 MB of data. This is because the multipart
 * upload API does not allow the uploading of more than one part smaller than 5 MB, which leads to fundamental limits
 * on what this class can accomplish. If order of data is important to you, then either use only one stream or ensure
 * that you write at least 5 MB to every stream.
 * <p>
 * While performing the multipart upload this class will create instances of {@link CreateMultipartUploadRequest},
 * {@link UploadPartRequest}, and {@link CompleteMultipartUploadRequest}, fill in the essential details, and send them
 * off. If you need to add additional details then override the appropriate {@code customise*Request} methods and
 * set the required properties within. Note that if no data is written (i.e. the object body is empty) then a normal (not multipart) upload will be performed and {@code customisePutEmptyObjectRequest} will be called instead.
 * <p>
 * This class does not perform retries when uploading. If an exception is thrown at any stage the upload will be aborted and the
 * exception rethrown, wrapped in a {@code RuntimeException}.
 * <p>
 * You can configure the upload process by calling any of the chaining setter methods {@link StreamTransferManager#numStreams(int)}, {@link StreamTransferManager#numUploadThreads(int)}, {@link StreamTransferManager#queueCapacity(int)}, or {@link StreamTransferManager#partSize(long)} before calling {@code getMultiPartOutputStreams}. Parts that have been produced sit in a queue of specified capacity while they wait for a thread to upload them.
 * The worst case memory usage is {@code (numUploadThreads + queueCapacity) * partSize + numStreams * (partSize + 6MB)},
 * while higher values for these first three parameters may lead to better resource usage and throughput.
 * If you are uploading very large files, you may need to increase the part size - see {@link StreamTransferManager#partSize(long)} for details.
 *
 * @author Alex Hall
 */
// @formatter:on
public class StreamTransferManager {

    private static final Logger log = LoggerFactory.getLogger(StreamTransferManager.class);

    public static final int MB = 1024 * 1024;

    protected final String bucketName;
    protected final String putKey;
    protected final S3Client s3Client;
    protected String uploadId;
    protected int numStreams = 1;
    protected int numUploadThreads = 1;
    protected int queueCapacity = 1;
    protected int partSize = 5 * MB;
    protected boolean checkIntegrity = false;
    private final List<CompletedPart> partETags = Collections.synchronizedList(new ArrayList<>());
    private List<MultiPartOutputStream> multiPartOutputStreams;
    private ExecutorServiceResultsHandler<Void> executorServiceResultsHandler;
    private ClosableQueue<StreamPart> queue;
    private int finishedCount = 0;
    private StreamPart leftoverStreamPart = null;
    private final Object leftoverStreamPartLock = new Object();
    private boolean isAborting = false;
    private static final int MAX_PART_NUMBER = 10000;

    public StreamTransferManager(String bucketName,
                                 String putKey,
                                 S3Client s3Client) {
        this.bucketName = bucketName;
        this.putKey = putKey;
        this.s3Client = s3Client;
    }

    /**
     * Sets the number of {@link MultiPartOutputStream}s that will be created and returned by
     * {@link StreamTransferManager#getMultiPartOutputStreams()} for you to write to.
     * <p>
     * By default this is 1, increase it if you want to write to multiple streams from different
     * threads in parallel.
     * <p>
     * If you are writing large files with many streams, you may need to increase the part size
     * to avoid running out of part numbers - see {@link StreamTransferManager#partSize(long)}
     * for more details.
     * <p>
     * Each stream may hold up to {@link StreamTransferManager#partSize(long)} + 6MB
     * in memory at a time.
     *
     * @return this {@code StreamTransferManager} for chaining.
     * @throws IllegalArgumentException if the argument is less than 1.
     * @throws IllegalStateException    if {@link StreamTransferManager#getMultiPartOutputStreams} has already
     *                                  been called, initiating the upload.
     */
    public StreamTransferManager numStreams(int numStreams) {
        ensureCanSet();
        if (numStreams < 1) {
            throw new IllegalArgumentException("There must be at least one stream");
        }
        this.numStreams = numStreams;
        return this;
    }

    /**
     * Sets the number of threads that will be created to upload the data in parallel to S3.
     * <p>
     * By default this is 1, increase it if uploading is a speed bottleneck and you have network
     * bandwidth to spare.
     * <p>
     * Each thread may hold up to {@link StreamTransferManager#partSize(long)}
     * in memory at a time.
     *
     * @return this {@code StreamTransferManager} for chaining.
     * @throws IllegalArgumentException if the argument is less than 1.
     * @throws IllegalStateException    if {@link StreamTransferManager#getMultiPartOutputStreams} has already
     *                                  been called, initiating the upload.
     */
    public StreamTransferManager numUploadThreads(int numUploadThreads) {
        ensureCanSet();
        if (numUploadThreads < 1) {
            throw new IllegalArgumentException("There must be at least one upload thread");
        }
        this.numUploadThreads = numUploadThreads;
        return this;
    }

    /**
     * Sets the capacity of the queue where completed parts from the output streams will sit
     * waiting to be taken by the upload threads.
     * <p>
     * By default this is 1, increase it if you want to help your threads which write
     * to the streams be consistently busy instead of blocking waiting for upload threads.
     * <p>
     * Each part sitting in the queue will hold {@link StreamTransferManager#partSize(long)} bytes
     * in memory at a time.
     *
     * @return this {@code StreamTransferManager} for chaining.
     * @throws IllegalArgumentException if the argument is less than 1.
     * @throws IllegalStateException    if {@link StreamTransferManager#getMultiPartOutputStreams} has already
     *                                  been called, initiating the upload.
     */
    public StreamTransferManager queueCapacity(int queueCapacity) {
        ensureCanSet();
        if (queueCapacity < 1) {
            throw new IllegalArgumentException("The queue capacity must be at least 1");
        }
        this.queueCapacity = queueCapacity;
        return this;
    }

    /**
     * Sets the size in MB of the parts to be uploaded to S3.
     * <p>
     * By default this is 5, which is the minimum that AWS allows. You may need to increase
     * it if you are uploading very large files or writing to many output streams.
     * <p>
     * AWS allows up to 10,000 parts to be uploaded for a single object, and each part must be
     * identified by a unique number from 1 to 10,000. These part numbers are allocated evenly
     * by the manager to each output stream. Therefore the maximum amount of data that can be
     * written to a stream is {@code 10000/numStreams * partSize}. If you try to write more,
     * an {@code IndexOutOfBoundsException} will be thrown.
     * The total object size can be at most 5 TB, so if you're using just one stream,
     * there is no reason to set this higher than 525. If you're using more streams, you may want
     * a higher value in case some streams get more data than others.
     * <p>
     * Increasing this value will of course increase memory usage.
     *
     * @return this {@code StreamTransferManager} for chaining.
     * @throws IllegalArgumentException if the argument is less than 5.
     * @throws IllegalArgumentException if the resulting part size in bytes cannot fit in a 32 bit int.
     * @throws IllegalStateException    if {@link StreamTransferManager#getMultiPartOutputStreams} has already
     *                                  been called, initiating the upload.
     */
    // partSize is a long here in case of a mistake on the user's part before calling this method.
    public StreamTransferManager partSize(long partSize) {
        ensureCanSet();
        partSize *= MB;
        if (partSize < MultiPartOutputStream.S3_MIN_PART_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "The given part size (%d) is less than 5 MB.", partSize));
        }
        if (partSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(
                    "The given part size (%d) is too large as it does not fit in a 32 bit int", partSize));
        }
        this.partSize = (int) partSize;
        return this;
    }

    /**
     * Sets whether a data integrity check should be performed during and after upload.
     * <p>
     * By default this is disabled.
     * <p>
     * The integrity check consists of two steps. First, each uploaded part
     * is verified by setting the <b>Content-MD5</b>
     * header for Amazon S3 to check against its own hash. If they don't match, the AWS SDK
     * will throw an exception. The header value is the
     * base64-encoded 128-bit MD5 digest of the part body.
     * <p>
     * The second step is to ensure integrity of the final object merged from the uploaded parts.
     * This is achieved by comparing the expected ETag value with the actual ETag returned by S3.
     * However, the ETag value is not a MD5 hash. When S3 combines the parts of a multipart upload
     * into the final object, the ETag value is set to the hex-encoded MD5 hash of the concatenated
     * binary-encoded MD5 hashes of each part followed by "-" and the number of parts, for instance:
     * <pre>57f456164b0e5f365aaf9bb549731f32-95</pre>
     * <b>Note that AWS doesn't document this, so their hashing algorithm might change without
     * notice which would lead to false alarm exceptions.
     * </b>
     * If the ETags don't match, an {@link IntegrityCheckException} will be thrown after completing
     * the upload. This will not abort or revert the upload.
     *
     * @param checkIntegrity <code>true</code> if data integrity should be checked
     * @return this {@code StreamTransferManager} for chaining.
     * @throws IllegalStateException if {@link StreamTransferManager#getMultiPartOutputStreams} has already
     *                               been called, initiating the upload.
     */
    public StreamTransferManager checkIntegrity(boolean checkIntegrity) {
        ensureCanSet();
        if (checkIntegrity) {
            Utils.md5();  // check that algorithm is available
        }
        this.checkIntegrity = checkIntegrity;
        return this;
    }

    private void ensureCanSet() {
        if (queue != null) {
            abort();
            throw new IllegalStateException("Setters cannot be called after getMultiPartOutputStreams");
        }

    }

    /**
     * Deprecated constructor kept for backward compatibility. Use {@link StreamTransferManager#StreamTransferManager(String, String, S3Client)} and then chain the desired setters.
     */
    @Deprecated
    public StreamTransferManager(String bucketName,
                                 String putKey,
                                 S3Client s3Client,
                                 int numStreams,
                                 int numUploadThreads,
                                 int queueCapacity,
                                 int partSize) {
        this(bucketName, putKey, s3Client);
        numStreams(numStreams);
        numUploadThreads(numUploadThreads);
        queueCapacity(queueCapacity);
        partSize(partSize);
    }

    /**
     * Get the list of output streams to write to.
     * <p>
     * The first call to this method initiates the multipart upload.
     * All setter methods must be called before this.
     */
    public List<MultiPartOutputStream> getMultiPartOutputStreams() {
        if (multiPartOutputStreams != null) {
            return multiPartOutputStreams;
        }

        queue = new ClosableQueue<>(queueCapacity);
        log.debug("Initiating multipart upload to {}/{}", bucketName, putKey);
        CreateMultipartUploadRequest initRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName).key(putKey).applyMutation(this::customiseInitiateRequest).build();
        CreateMultipartUploadResponse initResponse = s3Client.createMultipartUpload(initRequest);
        uploadId = initResponse.uploadId();
        log.info("Initiated multipart upload to {}/{} with full ID {}", bucketName, putKey, uploadId);
        try {
            multiPartOutputStreams = new ArrayList<>();
            ExecutorService threadPool = Executors.newFixedThreadPool(numUploadThreads);

            int partNumberStart = 1;

            for (int i = 0; i < numStreams; i++) {
                int partNumberEnd = (i + 1) * MAX_PART_NUMBER / numStreams + 1;
                MultiPartOutputStream multiPartOutputStream = new MultiPartOutputStream(partNumberStart, partNumberEnd, partSize, queue);
                partNumberStart = partNumberEnd;
                multiPartOutputStreams.add(multiPartOutputStream);
            }

            executorServiceResultsHandler = new ExecutorServiceResultsHandler<>(threadPool);
            for (int i = 0; i < numUploadThreads; i++) {
                executorServiceResultsHandler.submit(new UploadTask());
            }
            executorServiceResultsHandler.finishedSubmitting();
        } catch (Throwable e) {
            throw abort(e);
        }

        return multiPartOutputStreams;
    }

    /**
     * Blocks while waiting for the threads uploading the contents of the streams returned
     * by {@link StreamTransferManager#getMultiPartOutputStreams()} to finish, then sends a request to S3 to complete
     * the upload. For the former to complete, it's essential that every stream is closed, otherwise the upload
     * threads will block forever waiting for more data.
     */
    public void complete() {
        try {
            log.debug("{}: Waiting for pool termination", this);
            executorServiceResultsHandler.awaitCompletion();
            log.debug("{}: Pool terminated", this);
            if (leftoverStreamPart != null) {
                log.info("{}: Uploading leftover stream {}", this, leftoverStreamPart);
                uploadStreamPart(leftoverStreamPart);
                log.debug("{}: Leftover uploaded", this);
            }
            log.debug("{}: Completing", this);
            if (partETags.isEmpty()) {
                log.debug("{}: Aborting upload of empty stream", this);
                abort();
                log.info("{}: Putting empty object", this);
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(putKey)
                        .contentLength(0L)
                        .applyMutation(this::customisePutEmptyObjectRequest)
                        .build();
                s3Client.putObject(request, RequestBody.empty());
            } else {
                List<CompletedPart> sortedParts = new ArrayList<CompletedPart>(partETags);
                Collections.sort(sortedParts, new PartNumberComparator());
                CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(putKey)
                        .uploadId(uploadId)
                        .multipartUpload(b -> b.parts(sortedParts))
                        .applyMutation(this::customiseCompleteRequest)
                        .build();
                CompleteMultipartUploadResponse completeMultipartUploadResult = s3Client.completeMultipartUpload(completeRequest);
                if (checkIntegrity) {
                    checkCompleteFileIntegrity(completeMultipartUploadResult.eTag(), sortedParts);
                }
            }
            log.info("{}: Completed", this);
        } catch (IntegrityCheckException e) {
            // Nothing to abort. Upload has already finished.
            throw e;
        } catch (Throwable e) {
            throw abort(e);
        }
    }

    private void checkCompleteFileIntegrity(String s3ObjectETag, List<CompletedPart> sortedParts) {
        String expectedETag = computeCompleteFileETag(sortedParts);
        if (!expectedETag.equals(s3ObjectETag)) {
            throw new IntegrityCheckException(String.format(
                    "File upload completed, but integrity check failed. Expected ETag: %s but actual is %s",
                    expectedETag, s3ObjectETag));
        }
    }

    private String computeCompleteFileETag(List<CompletedPart> parts) {
        // When S3 combines the parts of a multipart upload into the final object, the ETag value is set to the
        // hex-encoded MD5 hash of the concatenated binary-encoded (raw bytes) MD5 hashes of each part followed by
        // "-" and the number of parts.
        MessageDigest md = Utils.md5();
        for (CompletedPart partETag : parts) {
            md.update(BinaryUtils.fromHex(partETag.eTag()));
        }
        // Represent byte array as a 32-digit number hexadecimal format followed by "-<partCount>".
        return String.format("%032x-%d", new BigInteger(1, md.digest()), parts.size());
    }

    /**
     * Aborts the upload and rethrows the argument, wrapped in a RuntimeException if necessary.
     * Write {@code throw abort(e)} to make it clear to the compiler and readers that the code
     * stops here.
     */
    public RuntimeException abort(Throwable t) {
        if (!isAborting) {
            log.error("Aborting {} due to error: {}", this, t.toString());
        }
        abort();
        if (t instanceof Error) {
            throw (Error) t;

        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;

        } else if (t instanceof InterruptedException) {
            throw Utils.runtimeInterruptedException((InterruptedException) t);

        } else {
            throw new RuntimeException(t);
        }
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
        if (executorServiceResultsHandler != null) {
            executorServiceResultsHandler.abort();
        }
        if (queue != null) {
            queue.close();
        }
        if (uploadId != null) {
            log.debug("{}: Aborting", this);
            AbortMultipartUploadRequest abortMultipartUploadRequest = AbortMultipartUploadRequest.builder()
                    .bucket(bucketName).key(putKey).uploadId(uploadId).build();
            s3Client.abortMultipartUpload(abortMultipartUploadRequest);
            log.info("{}: Aborted", this);
        }
    }

    private class UploadTask implements Callable<Void> {

        @Override
        public Void call() {
            try {
                while (true) {
                    StreamPart part;
                    //noinspection SynchronizeOnNonFinalField
                    synchronized (queue) {
                        if (finishedCount < multiPartOutputStreams.size()) {
                            part = queue.take();
                            if (part == StreamPart.POISON) {
                                finishedCount++;
                                continue;
                            }
                        } else {
                            break;
                        }
                    }
                    if (part.size() < MultiPartOutputStream.S3_MIN_PART_SIZE) {
                    /*
                    Each stream does its best to avoid producing parts smaller than 5 MB, but if a user doesn't
                    write that much data there's nothing that can be done. These are considered 'leftover' parts,
                    and must be merged with other leftovers to try producing a part bigger than 5 MB which can be
                    uploaded without problems. After the threads have completed there may be at most one leftover
                    part remaining, which S3 can accept. It is uploaded in the complete() method.
                    */
                        log.debug("{}: Received part {} < 5 MB that needs to be handled as 'leftover'", this, part);
                        StreamPart originalPart = part;
                        part = null;
                        synchronized (leftoverStreamPartLock) {
                            if (leftoverStreamPart == null) {
                                leftoverStreamPart = originalPart;
                                log.debug("{}: Created new leftover part {}", this, leftoverStreamPart);
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
                                log.debug("{}: Merged with existing leftover part to create {}", this, leftoverStreamPart);
                                if (leftoverStreamPart.size() >= MultiPartOutputStream.S3_MIN_PART_SIZE) {
                                    log.debug("{}: Leftover part can now be uploaded as normal and reset", this);
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
            } catch (Throwable t) {
                throw abort(t);
            }

            return null;
        }

    }

    private void uploadStreamPart(StreamPart part) {
        log.debug("{}: Uploading {}", this, part);

        UploadPartRequest.Builder builder = UploadPartRequest.builder()
                .bucket(bucketName)
                .key(putKey)
                .uploadId(uploadId)
                .partNumber(part.getPartNumber());
        if(checkIntegrity) {
            builder.contentMD5(part.getMD5Digest());
        }
        UploadPartRequest uploadRequest = builder
                .applyMutation(this::customiseUploadPartRequest)
                .build();

        UploadPartResponse uploadPartResult = s3Client.uploadPart(
                uploadRequest,
                RequestBody.fromInputStream(part.getInputStream(), part.size()));
        CompletedPart partETag = CompletedPart.builder()
                .partNumber(part.getPartNumber()).eTag(uploadPartResult.eTag()).build();
        partETags.add(partETag);
        log.info("{}: Finished uploading {}", this, part);
    }

    @Override
    public String toString() {
        return String.format("[Manager uploading to %s/%s with id %s]",
                bucketName, putKey, Utils.skipMiddle(String.valueOf(uploadId), 21));
    }

    // These methods are intended to be overridden for more specific interactions with the AWS API.

    @SuppressWarnings("unused")
    public void customiseInitiateRequest(CreateMultipartUploadRequest.Builder requestBuilder) {
    }

    @SuppressWarnings("unused")
    public void customiseUploadPartRequest(UploadPartRequest.Builder requestBuilder) {
    }

    @SuppressWarnings("unused")
    public void customiseCompleteRequest(CompleteMultipartUploadRequest.Builder requestBuilder) {
    }

    @SuppressWarnings("unused")
    public void customisePutEmptyObjectRequest(PutObjectRequest.Builder requestBuilder) {
    }

    private static class PartNumberComparator implements Comparator<CompletedPart> {
        @Override
        public int compare(CompletedPart o1, CompletedPart o2) {
            int partNumber1 = o1.partNumber();
            int partNumber2 = o2.partNumber();

            if (partNumber1 == partNumber2) {
                return 0;
            }
            return partNumber1 > partNumber2 ? 1 : -1;
        }
    }
}
