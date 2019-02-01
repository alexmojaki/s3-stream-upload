package alex.mojaki.s3upload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.GZIPOutputStream;

/**
 * A ByteArrayOutputStream with some useful additional functionality.
 */
class ConvertibleOutputStream extends ByteArrayOutputStream {

    private static final Logger log = LoggerFactory.getLogger(ConvertibleOutputStream.class);
    private boolean compressOnTheFly;
    private int compressedSize;

    public ConvertibleOutputStream(int initialCapacity) {
        super(initialCapacity);
    }
    public ConvertibleOutputStream(int initialCapacity, boolean compressOnTheFly) {
        super(initialCapacity);
        this.compressOnTheFly = compressOnTheFly;
    }

    /**
     * Creates an InputStream sharing the same underlying byte array, reducing memory usage and copying time.
     */
    public InputStream toInputStream() throws IOException {
        if (compressOnTheFly) {
            byte[] out = gzipCompress(buf, 0, count);
            compressedSize = out.length;
            return new ByteArrayInputStream(out);
        } else {
            return new ByteArrayInputStream(buf, 0, count);
        }
    }

    /**
     * Truncates this stream to a given size and returns a new stream containing a copy of the remaining data.
     *
     * @param countToKeep number of bytes to keep in this stream, starting from the first written byte.
     * @param initialCapacityForNewStream buffer capacity to construct the new stream (NOT the number of bytes
     *                                    that the new stream will take from this one)
     * @return a new stream containing all the bytes previously contained in this one, i.e. from countToKeep + 1 onwards.
     */
    public ConvertibleOutputStream split(int countToKeep, int initialCapacityForNewStream) {
        int newCount = count - countToKeep;
        log.debug("Splitting stream of size {} into parts with sizes {} and {}", count, countToKeep, newCount);
        initialCapacityForNewStream = Math.max(initialCapacityForNewStream, newCount);
        ConvertibleOutputStream newStream = new ConvertibleOutputStream(initialCapacityForNewStream, compressOnTheFly);
        newStream.write(buf, countToKeep, newCount);
        count = countToKeep;
        return newStream;
    }

    private static byte[] gzipCompress(byte[] uncompressedData, int start, int count) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(uncompressedData.length);
        GZIPOutputStream gzipOS = new GZIPOutputStream(bos);
        gzipOS.write(uncompressedData, start, count);
        gzipOS.close();
        return bos.toByteArray();
    }

    /**
     * Concatenates the given stream to this stream.
     */
    public void append(ConvertibleOutputStream otherStream) {
        try {
            otherStream.writeTo(this);
        } catch (IOException e) {

            // Should never happen because these are all ByteArrayOutputStreams
            throw new AssertionError(e);
        }
    }

    public int getCompressedSize() {
        return compressedSize;
    }

}