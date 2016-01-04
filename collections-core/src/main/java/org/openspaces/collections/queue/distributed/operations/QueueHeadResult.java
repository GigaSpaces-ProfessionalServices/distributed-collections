package org.openspaces.collections.queue.distributed.operations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static org.openspaces.collections.util.SerializationUtils.readNullableObject;
import static org.openspaces.collections.util.SerializationUtils.writeNullableObject;

public class QueueHeadResult implements Externalizable {

    private boolean queueEmpty;
    private Long headIndex;
    private boolean removedIndexesChanged;

    public QueueHeadResult() {
    }

    /**
     * Creates empty queue result
     *
     * @return result
     */
    public static QueueHeadResult emptyQueueResult() {
        return headIndexResult(true, null, false);
    }

    /**
     * Creates empty queue result with new head index
     *
     * @return result
     */
    public static QueueHeadResult emptyQueueResult(Long index, boolean removedIndexesChanged) {
        return headIndexResult(true, index, removedIndexesChanged);
    }

    /**
     * Creates non-empty queue result with new head index
     *
     * @return result
     */
    public static QueueHeadResult headIndexResult(Long index, boolean removedIndexesChanged) {
        return headIndexResult(false, index, removedIndexesChanged);
    }

    private static QueueHeadResult headIndexResult(boolean queueEmpty, Long index, boolean removedIndexesChanged) {
        QueueHeadResult result = new QueueHeadResult();
        result.queueEmpty = queueEmpty;
        result.headIndex = index;
        result.removedIndexesChanged = removedIndexesChanged;
        return result;
    }

    public boolean isQueueEmpty() {
        return queueEmpty;
    }

    public Long getHeadIndex() {
        return headIndex;
    }

    public boolean getRemovedIndexesChanged() {
        return removedIndexesChanged;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(isQueueEmpty());
        writeNullableObject(out, getHeadIndex());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.queueEmpty = in.readBoolean();
        this.headIndex = readNullableObject(in);
    }
}
