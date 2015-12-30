/**
 * 
 */
package org.openspaces.collections.queue.operations;

import com.esotericsoftware.kryo.Kryo;
import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

import java.util.List;

import static org.openspaces.collections.queue.data.EmbeddedQueue.QUEUE_CONTAINER_PATH;
import static org.openspaces.collections.queue.data.EmbeddedQueueContainer.ITEMS_PATH;
import static org.openspaces.collections.queue.data.EmbeddedQueueContainer.SIZE_PATH;

/**
 * @author Svitlana_Pogrebna
 *
 */
public abstract class EmbeddedChangeOperation<T> extends CustomChangeOperation {

    private static final long serialVersionUID = 1L;

    protected static final String FULL_QUEUE_PATH = QUEUE_CONTAINER_PATH + "." + ITEMS_PATH;
    protected static final String FULL_SIZE_PATH = QUEUE_CONTAINER_PATH + "." + SIZE_PATH;

    @Override
    public Object change(MutableServerEntry entry) {
        final List<Object> originalQueue = (List<Object>) entry.getPathValue(FULL_QUEUE_PATH);
        final List<Object> items = new Kryo().copyShallow(originalQueue);

        final T result = change(entry, items);

        entry.setPathValue(FULL_SIZE_PATH, items.size());
        entry.setPathValue(FULL_QUEUE_PATH, items);
        return new EmbeddedQueueChangeResult<T>(result);
    }

    protected abstract T change(MutableServerEntry entry, List<Object> items);
}
