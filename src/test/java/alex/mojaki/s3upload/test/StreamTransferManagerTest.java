package alex.mojaki.s3upload.test;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.utils.IoUtils;

import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Change setUp to point to your S3 details.
 */
public class StreamTransferManagerTest {

    private String region;
    private S3Client s3Client;
    private String containerName;
    private String key;

    @Before
    public void setUp() throws Exception {
        region = "us-east-1";
        containerName = "INSERT_BUCKET_NAME_HERE";
        key = "INSERT_KEY_HERE";

        s3Client = S3Client.builder()
                .httpClientBuilder(ApacheHttpClient.builder())
                .region(Region.of(region))
                .build();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testNormal() throws Exception {
        testTransferManager(1000000, false, true);
    }

    @Test
    public void testSerial() throws Exception {
        testTransferManager(1000000, false, false);
    }

    @Test
    public void testEmpty() throws Exception {
        testTransferManager(0, false, true);
    }

    @Test(expected = RuntimeException.class)
    public void testException() throws Exception {
        testTransferManager(10000000, true, true);
    }

    @Test(expected = IllegalStateException.class)
    public void testExceptionSerial() throws Exception {
        testTransferManager(10000000, true, false);
    }

    private void testTransferManager(final int numLines, final boolean throwException, boolean parallel) throws Exception {
        int numStreams = 2;
        final StreamTransferManager manager = new StreamTransferManager(containerName, key, s3Client) {

            @Override
            public void customiseUploadPartRequest(UploadPartRequest.Builder requestBuilder) {
                if (throwException) {
                    throw new RuntimeException("Testing failure");
                }
            }
        }.numStreams(numStreams)
                .numUploadThreads(2)
                .queueCapacity(2)
                .partSize(10)
                .checkIntegrity(true);

        final List<MultiPartOutputStream> streams = manager.getMultiPartOutputStreams();
        List<StringBuilder> builders = new ArrayList<StringBuilder>(numStreams);
        ExecutorService pool = Executors.newFixedThreadPool(numStreams);
        for (int i = 0; i < numStreams; i++) {
            final int streamIndex = i;
            final StringBuilder builder = new StringBuilder();
            builders.add(builder);
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    MultiPartOutputStream outputStream = streams.get(streamIndex);
                    for (int lineNum = 0; lineNum < numLines; lineNum++) {
                        String line = String.format("Stream %d, line %d\n", streamIndex, lineNum);
                        outputStream.write(line.getBytes());
                        builder.append(line);
                    }
                    outputStream.close();
                }
            };
            if (parallel) {
                pool.submit(task);
            } else {
                task.run();
            }
        }
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        manager.complete();

        for (int i = 1; i < numStreams; i++) {
            builders.get(0).append(builders.get(i));
        }

        String expectedResult = builders.get(0).toString();

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(containerName)
                .key(key)
                .build();

        try (ResponseInputStream<GetObjectResponse> responseInputStream = s3Client
                .getObject(getObjectRequest)) {
            String result = IoUtils.toUtf8String(responseInputStream);
            Assert.assertEquals(expectedResult, result);
        }
    }
}
