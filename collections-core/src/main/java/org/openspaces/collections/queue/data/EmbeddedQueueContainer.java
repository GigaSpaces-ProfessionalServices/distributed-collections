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
    
    public static final String QUEUE_PATH = "queue";
    public static final String QUEUE_SIZE_PATH = "size";
    
    private Queue<Object> queue;
    private Integer size;
    
    public EmbeddedQueueContainer() {
    }
    
    public EmbeddedQueueContainer(Queue<Object> queue) {
        this.queue = Objects.requireNonNull(queue, "'queue' parameter must not be null");
        this.size = queue.size();
    }
    
    public Queue<Object> getQueue() {
        return queue;
    }

    public void setQueue(Queue<Object> queue) {
        this.queue = queue;
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
        setQueue((Queue<Object>)in.readObject());
        setSize(in.readInt());
    }
}
