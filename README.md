# S3 Stream Upload

This library allows you to efficiently stream data to a location on AWS S3 in Java. The S3 API requires that a content
length be set before starting uploading, which is a problem when you want to calculate a large amount of data on the fly.
The standard Java AWS SDK will simply buffer all the data in memory so that it can calculate the length, which consumes
RAM and delays the upload. You can write the data to a temporary file but disk IO is slow. This library provides
an `OutputStream` that packages data written to it into chunks which are sent in a multipart upload. You can also use
several streams and upload the data in parallel.

The entrypoint is the class `StreamTransferManager`. Read more in the
[javadoc](http://alexmojaki.github.io/s3-stream-upload/javadoc/apidocs/alex/mojaki/s3upload/StreamTransferManager.html),
including a usage example.

This is available from maven central. You can include it as a dependency in `pom.xml`:

```
<dependency>
    <groupId>com.github.alexmojaki</groupId>
    <artifactId>s3-stream-upload</artifactId>
    <version>1.0.1</version>
</dependency>
```
