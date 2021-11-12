# S3 Stream Upload

This library allows you to efficiently stream large amounts of data to AWS S3 in Java without having to store the whole object in memory or use files. The S3 API requires that a content
length be set before starting uploading, which is a problem when you want to calculate a large amount of data on the fly.
The standard Java AWS SDK will simply buffer all the data in memory so that it can calculate the length, which consumes
RAM and delays the upload. You can write the data to a temporary file but disk IO is slow (if your data is already in a file, using this library is pointless). This library provides
an `OutputStream` that packages data written to it into chunks which are sent in a multipart upload. You can also use
several streams and upload the data in parallel.

The entrypoint is the class `StreamTransferManager`. Read more in the
[javadoc](http://alexmojaki.github.io/s3-stream-upload/javadoc/apidocs/alex/mojaki/s3upload/StreamTransferManager.html),
including a usage example.

This is available from [maven central](https://mvnrepository.com/artifact/com.github.alexmojaki/s3-stream-upload/latest).

## Changelog

### 2.2.4

- [Abort multipart upload when content is empty](https://github.com/alexmojaki/s3-stream-upload/pull/41) thanks to @kcalcagno


### 2.2.3

- [Use String.valueOf to protect against null uploadId in toString](https://github.com/alexmojaki/s3-stream-upload/pull/38) thanks to @kcalcagno

### 2.2.2

- [Sort PartETag list to avoid an InvalidPartOrder error on complete (#34)](https://github.com/alexmojaki/s3-stream-upload/pull/34) thanks to @dana-katzenelson-livongo

### 2.2.1

- Exceptions during upload lead to an exception in writers instead of them getting stuck trying to put on the queue.
- Bump `aws-java-sdk` and `slf4j-api` dependency versions.

### 2.2.0

- [Add `checkIntegrity()` method](https://github.com/alexmojaki/s3-stream-upload/pull/26) thanks
to **@gkolakowski-ias**. This allows verifying the upload with MD5 hashes.

### 2.1.0

- Support uploading empty objects. In this case the user may want to override `customisePutEmptyObjectRequest`.
- Bump `aws-java-sdk` and `slf4j-api` dependency versions.

### 2.0.3

- Bump `aws-java-sdk` dependency version

### 2.0.2

- Avoid race condition in big uploads causing some parts to be missing from final completed upload. 

### 2.0.1

- Allow thrown `Error`s (e.g. OOM) to bubble up unchanged.

### 2.0.0

- The `checkSize()` method is now private as the user no longer needs to call it. You can remove all calls to it.
- The constructor from version 1 is now deprecated. Instead you should call the 3 parameter constructor which has the essentials, and optionally chain the desired builder style setters to configure.
