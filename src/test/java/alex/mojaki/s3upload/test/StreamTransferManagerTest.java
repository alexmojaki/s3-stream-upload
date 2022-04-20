package alex.mojaki.s3upload.test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * Stream transfer manager test class using a mocked AWS SDK S3Client
 */
@RunWith(Parameterized.class)
public class StreamTransferManagerTest {

    private final String containerName = "bucketName";
    private String key;
    private S3Client client;
    private int numLines;
    private int wantedUploadPartsCount;

    public StreamTransferManagerTest(int numLines, int wantedUploadPartsCount) {
        this.numLines = numLines;
        this.wantedUploadPartsCount = wantedUploadPartsCount;
        key = numLines + "-lines";
    }

    @Before
    public void setUp() throws Exception {
        CreateMultipartUploadResponse createMultipartUploadResponse = mock(CreateMultipartUploadResponse.class);
        when(createMultipartUploadResponse.uploadId()).thenReturn(UUID.randomUUID().toString());

        UploadPartResponse uploadPartResponse = mock(UploadPartResponse.class);
        when(uploadPartResponse.eTag()).thenReturn(UUID.randomUUID().toString());

        CompleteMultipartUploadResponse completeMultipartUploadResponse = mock(
                CompleteMultipartUploadResponse.class);

        client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(createMultipartUploadResponse);
                
        if(wantedUploadPartsCount > 0) {
            when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenReturn(uploadPartResponse);
            when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(completeMultipartUploadResponse);
        }
    }

    @Test
    public void testTransferManager() throws Exception {
        int numStreams = 2;
        int numUploadThreads = 2;
        int queueCapacity = 2;
        int partSize = 10;
        final StreamTransferManager manager = new StreamTransferManager(containerName, key, client)
                .numStreams(numStreams)
                .numUploadThreads(numUploadThreads)
                .queueCapacity(queueCapacity)
                .partSize(partSize);

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
        if(wantedUploadPartsCount > 0) {
            verify(client, times(wantedUploadPartsCount)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
            verify(client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        }
    }

    @Parameterized.Parameters
    public static Collection<Integer[]> input() {
        return Arrays.asList(new Integer[][]{{1000000, 4}, {500000, 2}, {100000,1}, {1,1}, {0,0}});
    }
}
