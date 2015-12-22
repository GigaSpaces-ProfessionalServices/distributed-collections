package org.openspaces.collections.queue.operations;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;
import org.openspaces.collections.queue.data.QueueData;

import java.io.Serializable;

/**
 * @author Oleksiy_Dyagilev
 */
public class OfferOperation extends CustomChangeOperation {

    private static final String HEAD_PATH = "head";
    private static final String TAIL_PATH = "tail";
    private static final String BOUNDED_PATH = "bounded";
    private static final String CAPACITY_PATH = "capacity";

    private final int itemsNumber;

    public OfferOperation(int itemsNumber) {
        this.itemsNumber = itemsNumber;
    }

    @Override
    public Object change(MutableServerEntry entry) {
        Long tail = (Long) entry.getPathValue(TAIL_PATH);
        Long head = (Long) entry.getPathValue(HEAD_PATH);
        boolean bounded = (Boolean) entry.getPathValue(BOUNDED_PATH);
        int capacity = (Integer) entry.getPathValue(CAPACITY_PATH);

        if (hasSpaceAvailable(bounded, tail, head, capacity)) {
            long newTail = tail + itemsNumber;
            entry.setPathValue(TAIL_PATH, newTail);
            return Result.changed(newTail);
        } else {
            return Result.notChanged();
        }
    }

    private boolean hasSpaceAvailable(boolean bounded, Long tail, Long head, int capacity) {
        return !bounded || (capacity >= (tail - head + itemsNumber));
    }

    @Override
    public String getName() {
        return "offer";
    }

    /**
     * Operation result
     */
    public static final class Result implements Serializable {
        private boolean changed;
        private long newTail;

        public Result() {
            // for ser.
        }

        public static Result changed(long newTail) {
            Result result = new Result();
            result.changed = true;
            result.newTail = newTail;
            return result;
        }

        public static Result notChanged() {
            return new Result();
        }

        public boolean isChanged() {
            return changed;
        }

        public long getNewTail() {
            return newTail;
        }
    }
}
