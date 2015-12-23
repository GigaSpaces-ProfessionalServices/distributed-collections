package org.openspaces.collections.queue.data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

/**
 * @author Oleksiy_Dyagilev
 */
public class QueueItemKey implements Externalizable {

    private static final long serialVersionUID = 1L;
    
    private String queueName;
    private Long queueIndex;

    public QueueItemKey() {
    }

    public QueueItemKey(String queueName, Long queueIndex) {
        this.queueName = Objects.requireNonNull(queueName, "'queueName' must not be null");
        this.queueIndex = Objects.requireNonNull(queueIndex, "'queueIndex' must not be null");
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public Long getQueueIndex() {
        return queueIndex;
    }

    public void setQueueIndex(Long queueIndex) {
        this.queueIndex = queueIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueueItemKey that = (QueueItemKey) o;

        if (queueName != null ? !queueName.equals(that.queueName) : that.queueName != null) return false;
        return !(queueIndex != null ? !queueIndex.equals(that.queueIndex) : that.queueIndex != null);

    }

    @Override
    public int hashCode() {
        int result = queueName != null ? queueName.hashCode() : 0;
        result = 31 * result + (queueIndex != null ? queueIndex.hashCode() : 0);
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(getQueueName());
        out.writeLong(getQueueIndex());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
       setQueueName(in.readUTF());
       setQueueIndex(in.readLong());
    }
}
