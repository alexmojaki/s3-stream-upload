package alex.mojaki.s3upload;

import java.util.concurrent.ArrayBlockingQueue;

public class ClosableQueue<T> extends ArrayBlockingQueue<T> {
    private volatile boolean closed = false;

    public ClosableQueue(int capacity) {
        super(capacity);
    }

    public void close() {
        closed = true;
    }

    @Override
    public void put(T t) throws InterruptedException {
        if (closed) {
            throw new IllegalStateException(
                    "The queue is now closed due to an error elsewhere"
            );
        }
        super.put(t);
    }
}
