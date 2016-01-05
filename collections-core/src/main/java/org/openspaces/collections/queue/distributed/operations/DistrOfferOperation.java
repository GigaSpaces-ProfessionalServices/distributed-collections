package org.openspaces.collections.queue.distributed.operations;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static org.openspaces.collections.queue.distributed.data.DistrQueueMetadata.*;
import static org.openspaces.collections.util.SerializationUtils.readNullableObject;
import static org.openspaces.collections.util.SerializationUtils.writeNullableObject;

/**
 * @author Oleksiy_Dyagilev
 */
public class DistrOfferOperation extends CustomChangeOperation {

    private final int itemsNumber;

    public DistrOfferOperation(int itemsNumber) {
        this.itemsNumber = itemsNumber;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object change(MutableServerEntry entry) {
        Long tail = (Long) entry.getPathValue(TAIL_PATH);
        Long head = (Long) entry.getPathValue(HEAD_PATH);
        boolean bounded = (Boolean) entry.getPathValue(BOUNDED_PATH);
        int capacity = (Integer) entry.getPathValue(CAPACITY_PATH);
        int removedIndexesSize = (Integer) entry.getPathValue(REMOVED_INDEXES_SIZE_PATH);

        if (hasSpaceAvailable(bounded, tail, head, capacity, removedIndexesSize)) {
            long newTail = tail + itemsNumber;
            entry.setPathValue(TAIL_PATH, newTail);
            return Result.changed(newTail);
        } else {
            return Result.notChanged();
        }
    }

    private boolean hasSpaceAvailable(boolean bounded, Long tail, Long head, int capacity, int removedIndexesSize) {
        return !bounded || (capacity >= (tail - head + itemsNumber - removedIndexesSize));
    }

    @Override
    public String getName() {
        return "offer";
    }

    /**
     * Operation result
     */
    public static final class Result implements Externalizable {
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

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeBoolean(isChanged());
            writeNullableObject(out, getNewTail());
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.changed = in.readBoolean();
            this.newTail = readNullableObject(in);
        }
    }
}
