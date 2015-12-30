/**
 *
 */
package org.openspaces.collections.queue.operations;

import com.esotericsoftware.kryo.Kryo;
import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

import java.util.Queue;

import static org.openspaces.collections.queue.data.EmbeddedQueue.QUEUE_CONTAINER_PATH;
import static org.openspaces.collections.queue.data.EmbeddedQueueContainer.QUEUE_PATH;
import static org.openspaces.collections.queue.data.EmbeddedQueueContainer.QUEUE_SIZE_PATH;

/**
 * @author Svitlana_Pogrebna
 */
public abstract class EmbeddedChangeOperation<T> extends CustomChangeOperation {

    private static final long serialVersionUID = 1L;

    protected static final String FULL_QUEUE_PATH = QUEUE_CONTAINER_PATH + "." + QUEUE_PATH;
    protected static final String FULL_SIZE_PATH = QUEUE_CONTAINER_PATH + "." + QUEUE_SIZE_PATH;

    @Override
    public Object change(MutableServerEntry entry) {
        final Queue<Object> originalQueue = (Queue<Object>) entry.getPathValue(FULL_QUEUE_PATH);
        final Queue<Object> queue = new Kryo().copyShallow(originalQueue);

        final T result = change(queue);

        entry.setPathValue(FULL_SIZE_PATH, queue.size());
        entry.setPathValue(FULL_QUEUE_PATH, queue);
        return new EmbeddedQueueChangeResult<T>(result);
    }

    protected abstract T change(Queue<Object> queue);
}
