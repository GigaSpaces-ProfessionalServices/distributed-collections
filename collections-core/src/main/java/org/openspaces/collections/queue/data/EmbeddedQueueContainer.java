/**
 * 
 */
package org.openspaces.collections.queue.data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.Queue;

/**
 * @author Svitlana_Pogrebna
 *
 */
public class EmbeddedQueueContainer implements Externalizable {
    
    private Queue<byte[]> queue;
    private Integer size;
    
    public EmbeddedQueueContainer() {
    }
    
    public EmbeddedQueueContainer(Queue<byte[]> queue) {
        this.queue = Objects.requireNonNull(queue, "'queue' parameter must not be null");
        this.size = queue.size();
    }
    
    public Queue<byte[]> getQueue() {
        return queue;
    }

    public void setQueue(Queue<byte[]> queue) {
        this.queue = queue;
        this.size = queue.size();
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(getQueue());
        out.writeInt(getSize());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setQueue((Queue<byte[]>)in.readObject());
        setSize(in.readInt());
    }
}
