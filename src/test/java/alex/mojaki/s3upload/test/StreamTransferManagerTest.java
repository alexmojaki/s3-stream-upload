package alex.mojaki.s3upload.test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.stubbing.Answer;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.utils.BinaryUtils;

/**
 * Stream transfer manager test class using a mocked AWS SDK S3Client
 */
@RunWith(Parameterized.class)
public class StreamTransferManagerTest {

    private S3Client client;
    private final int numLines;
    private final int wantedUploadPartsCount;
    private final boolean checkIntegrity;

    public StreamTransferManagerTest(int numLines, boolean checkIntegrity, int wantedUploadPartsCount) {
        this.numLines = numLines;
        this.checkIntegrity = checkIntegrity;
        this.wantedUploadPartsCount = wantedUploadPartsCount;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> input() {
        Object[][] arr = {
            {1000000, false, 4},
            {1000000, true, 4},
            {500000, false, 2},
            {100000, false, 1},
            {1, false, 1},
            {0, false, 0}
        };
        return Arrays.asList(arr);
    }

    @Before
    public void setUp() {
        CreateMultipartUploadResponse createMultipartUploadResponse = mock(CreateMultipartUploadResponse.class);
        when(createMultipartUploadResponse.uploadId()).thenReturn(UUID.randomUUID().toString());

        client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(createMultipartUploadResponse);

        if (wantedUploadPartsCount == 0) {
            return;
        }

        Answer<UploadPartResponse> uploadPartAnswer = invocation -> {
            RequestBody requestBody = invocation.getArgument(1);
            try (InputStream is = requestBody.contentStreamProvider().newStream()) {
                String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(is);
                return UploadPartResponse.builder().eTag(md5).build();
            }
        };
        Answer<CompleteMultipartUploadResponse> completeMultipartUploadAnswer = invocation -> {
            CompleteMultipartUploadRequest uploadRequest = invocation.getArgument(0);
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.reset();
            List<CompletedPart> parts = uploadRequest.multipartUpload().parts();
            parts.forEach(part -> md.update(BinaryUtils.fromHex(part.eTag())));
            String eTag = String.format("%032x-%d", new BigInteger(1, md.digest()), parts.size());
            return CompleteMultipartUploadResponse.builder().eTag(eTag).build();
        };

        when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
                .thenAnswer(uploadPartAnswer);
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenAnswer(completeMultipartUploadAnswer);
    }

    @Test
    public void testTransferManager() throws Exception {
        int numStreams = 2;
        String key = numLines + "-lines";
        final StreamTransferManager manager = new StreamTransferManager("bucketName", key, client)
                .checkIntegrity(checkIntegrity)
                .numStreams(numStreams)
                .numUploadThreads(2)
                .queueCapacity(2)
                .partSize(10);

        final List<MultiPartOutputStream> streams = manager.getMultiPartOutputStreams();
        List<StringBuilder> builders = new ArrayList<>(numStreams);
        ExecutorService pool = Executors.newFixedThreadPool(numStreams);
        for (int i = 0; i < numStreams; i++) {
            final int streamIndex = i;
            final StringBuilder builder = new StringBuilder();
            builders.add(builder);
            pool.submit(() -> {
                MultiPartOutputStream outputStream = streams.get(streamIndex);
                for (int lineNum = 0; lineNum < numLines; lineNum++) {
                    String line = String.format("Stream %d, line %d\n", streamIndex, lineNum);
                    outputStream.write(line.getBytes());
                    builder.append(line);
                }
                outputStream.close();
            });
        }
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        manager.complete();

        verify(client).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        if (wantedUploadPartsCount > 0) {
            verify(client, times(wantedUploadPartsCount)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
            verify(client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        }
    }

}
