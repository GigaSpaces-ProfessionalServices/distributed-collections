package org.openspaces.collections.queue.operations;

import static org.openspaces.collections.util.SerializationUtils.readNullableObject;
import static org.openspaces.collections.util.SerializationUtils.writeNullableObject;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class QueueHeadResult implements Externalizable {
    
    private boolean queueEmpty;
    private Long headIndex;

    public QueueHeadResult() {
    }

    /**
     * Creates empty queue result
     * 
     * @return result
     */
    public static QueueHeadResult emptyQueueResult() {
        return headIndexResult(true, null);
    }
    
    /**
     * Creates empty queue result with new head index
     * 
     * @param index
     * @return result
     */
    public static QueueHeadResult emptyQueueResult(Long index) {
        return headIndexResult(true, index);
    }

    /**
     * Creates non-empty queue result with new head index
     * 
     * @param index
     * @return result
     */
    public static QueueHeadResult headIndexResult(Long index) {
        return headIndexResult(false, index);
    }
    
    private static QueueHeadResult headIndexResult(boolean queueEmpty, Long index) {
        QueueHeadResult result = new QueueHeadResult();
        result.queueEmpty = queueEmpty;
        result.headIndex = index;
        return result;
    }

    public boolean isQueueEmpty() {
        return queueEmpty;
    }

    public Long getHeadIndex() {
        return headIndex;
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
