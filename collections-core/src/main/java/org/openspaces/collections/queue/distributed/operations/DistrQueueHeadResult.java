package org.openspaces.collections.queue.distributed.operations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static org.openspaces.collections.util.SerializationUtils.readNullableObject;
import static org.openspaces.collections.util.SerializationUtils.writeNullableObject;

public class DistrQueueHeadResult implements Externalizable {

    private boolean queueEmpty;
    private Long headIndex;
    private boolean removedIndexesChanged;

    public DistrQueueHeadResult() {
    }

    /**
     * Creates empty queue result
     *
     * @return result
     */
    public static DistrQueueHeadResult emptyQueueResult() {
        return headIndexResult(true, null, false);
    }

    /**
     * Creates empty queue result with new head index
     *
     * @return result
     */
    public static DistrQueueHeadResult emptyQueueResult(Long index, boolean removedIndexesChanged) {
        return headIndexResult(true, index, removedIndexesChanged);
    }

    /**
     * Creates non-empty queue result with new head index
     *
     * @return result
     */
    public static DistrQueueHeadResult headIndexResult(Long index, boolean removedIndexesChanged) {
        return headIndexResult(false, index, removedIndexesChanged);
    }

    private static DistrQueueHeadResult headIndexResult(boolean queueEmpty, Long index, boolean removedIndexesChanged) {
        DistrQueueHeadResult result = new DistrQueueHeadResult();
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
