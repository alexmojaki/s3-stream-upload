package alex.mojaki.s3upload.test;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.inject.Module;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.gaul.s3proxy.S3Proxy;
import org.gaul.s3proxy.S3ProxyConstants;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.junit.*;
import org.junit.rules.ExpectedException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.awssdk.utils.IoUtils;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

/**
 * A WIP test using s3proxy to avoid requiring actually connecting to a real S3 bucket.
 */
public class StreamTransferManagerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private URI s3Endpoint;
    private S3Proxy s3Proxy;
    private BlobStoreContext context;
    private String containerName;
    private String key;
    private AwsBasicCredentials awsCreds;

    @Before
    public void setUp() throws Exception {
        Properties s3ProxyProperties = new Properties();
        try (InputStream is = Resources.asByteSource(Resources.getResource(
                "s3proxy.conf")).openStream()) {
            s3ProxyProperties.load(is);
        }

        String provider = s3ProxyProperties.getProperty(
                Constants.PROPERTY_PROVIDER);
        String identity = s3ProxyProperties.getProperty(
                Constants.PROPERTY_IDENTITY);
        String credential = s3ProxyProperties.getProperty(
                Constants.PROPERTY_CREDENTIAL);
        String endpoint = s3ProxyProperties.getProperty(
                Constants.PROPERTY_ENDPOINT);
        String s3Identity = s3ProxyProperties.getProperty(
                S3ProxyConstants.PROPERTY_IDENTITY);
        String s3Credential = s3ProxyProperties.getProperty(
                S3ProxyConstants.PROPERTY_CREDENTIAL);

        awsCreds = AwsBasicCredentials.create(s3Identity, s3Credential);
        s3Endpoint = new URI(s3ProxyProperties.getProperty(
                S3ProxyConstants.PROPERTY_ENDPOINT));
        String keyStorePath = s3ProxyProperties.getProperty(
                S3ProxyConstants.PROPERTY_KEYSTORE_PATH);
        String keyStorePassword = s3ProxyProperties.getProperty(
                S3ProxyConstants.PROPERTY_KEYSTORE_PASSWORD);
        String virtualHost = s3ProxyProperties.getProperty(
                S3ProxyConstants.PROPERTY_VIRTUAL_HOST);

        ContextBuilder builder = ContextBuilder
                .newBuilder(provider)
                .credentials(identity, credential)
                .modules(ImmutableList.<Module>of(new SLF4JLoggingModule()))
                .overrides(s3ProxyProperties);
        if (!Strings.isNullOrEmpty(endpoint)) {
            builder.endpoint(endpoint);
        }
        context = builder.build(BlobStoreContext.class);
        BlobStore blobStore = context.getBlobStore();
        containerName = createRandomContainerName();
        key = "stuff";
        blobStore.createContainerInLocation(null, containerName);

        S3Proxy.Builder s3ProxyBuilder = S3Proxy.builder()
                .blobStore(blobStore)
                .endpoint(s3Endpoint);
        if (s3Identity != null && s3Credential != null) {
            s3ProxyBuilder.awsAuthentication(s3Identity, s3Credential);
        }
        if (keyStorePath != null || keyStorePassword != null) {
            s3ProxyBuilder.keyStore(
                    Resources.getResource(keyStorePath).toString(),
                    keyStorePassword);
        }
        if (virtualHost != null) {
            s3ProxyBuilder.virtualHost(virtualHost);
        }
        s3Proxy = s3ProxyBuilder.build();
        s3Proxy.start();
        while (!s3Proxy.getState().equals(AbstractLifeCycle.STARTED)) {
            Thread.sleep(1);
        }

        // reset endpoint to handle zero port
        s3Endpoint = new URI(s3Endpoint.getScheme(), s3Endpoint.getUserInfo(),
                s3Endpoint.getHost(), s3Proxy.getPort(), s3Endpoint.getPath(),
                s3Endpoint.getQuery(), s3Endpoint.getFragment());
    }

    @After
    public void tearDown() throws Exception {
        if (s3Proxy != null) {
            s3Proxy.stop();
        }
        if (context != null) {
            context.getBlobStore().deleteContainer(containerName);
            context.close();
        }
    }

    @Test
    public void testTransferManager() throws Exception {
        testTransferManager(1000000);
        testTransferManager(0);
    }

    private void testTransferManager(final int numLines) throws Exception {
        S3Client client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .endpointOverride(s3Endpoint)
                .overrideConfiguration(b -> b.putAdvancedOption(
                        SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create()))
                .httpClient(ApacheHttpClient.builder().buildWithDefaults(
                        AttributeMap.builder()
                                .put(TRUST_ALL_CERTIFICATES, Boolean.TRUE)
                                .build()
                ))
//                .enablePathStyleAccess()
                .build();

        int numStreams = 2;
        final StreamTransferManager manager = new StreamTransferManager(containerName, key, client) {

//            @Override
//            public void customiseUploadPartRequest(UploadPartRequest request) {
//                /*
//                Workaround from https://github.com/andrewgaul/s3proxy/commit/50a302436271ec46ce81a415b4208b9e14fcaca4
//                to deal with https://github.com/andrewgaul/s3proxy/issues/80
//                 */
//                ObjectMetadata metadata = new ObjectMetadata();
//                metadata.setContentType("application/unknown");
//                request.setObjectMetadata(metadata);
//            }
        }.numStreams(numStreams)
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

        for (int i = 1; i < numStreams; i++) {
            builders.get(0).append(builders.get(i));
        }

        String expectedResult = builders.get(0).toString();

        ResponseInputStream<GetObjectResponse> responseInputStream = client.getObject(b -> b.bucket(containerName).key(key));
        String result = IoUtils.toUtf8String(responseInputStream);
        IoUtils.closeQuietly(responseInputStream, null);

        Assert.assertEquals(expectedResult, result);
    }

    private static String createRandomContainerName() {
        return "s3proxy-" + new Random().nextInt(Integer.MAX_VALUE);
    }

}
