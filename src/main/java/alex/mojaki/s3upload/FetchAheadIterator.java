package alex.mojaki.s3upload;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An abstract iterator that implements {@code next()} based on the assumption that {@code hasNext()}
 * is responsible for fetching and storing the next element in the iteration and
 * recording whether it will be returned.
 * This is convenient for cases when the only way to know if there is another element is
 * to try obtaining one.
 * The implementation of {@code next()} then enforces that the iterator is used in the standard fashion,
 * e.g. {@code while (iter.hasNext()) { doSomethingWith(iter.next()); }}.
 * {@code hasNext()} should be implemented as follows:
 * <ol>
 * <li>Obtain the next element if possible and assign it to the field {@code next}.</li>
 * <li>Assign the appropriate value to the boolean field {@code hasNext}.</li>
 * <li>Return {@code hasNext}.</li>
 * </ol>
 */
abstract class FetchAheadIterator<T> implements Iterator<T> {
    protected boolean hasNext = false;
    protected T next;

    @Override
    public T next() {
        if (!hasNext) {
            throw new NoSuchElementException("next() called without ensuring hasNext()");
        }
        hasNext = false;
        return next;
    }
}
