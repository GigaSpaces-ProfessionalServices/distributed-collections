/**
 *
 */
package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

import java.util.ArrayList;
import java.util.List;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.ITEMS_PATH;
import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.SIZE_PATH;

/**
 * @author Svitlana_Pogrebna
 */
public abstract class EmbeddedChangeOperation<T> extends CustomChangeOperation {

    private static final long serialVersionUID = 1L;

    @Override
    public Object change(MutableServerEntry entry) {
        final List<Object> originalQueue = (List<Object>) entry.getPathValue(ITEMS_PATH);
        final List<Object> items = new ArrayList<>(originalQueue);

        final T result = change(entry, items);

        entry.setPathValue(SIZE_PATH, items.size());
        entry.setPathValue(ITEMS_PATH, items);
        return new SerializableResult<T>(result);
    }

    protected abstract T change(MutableServerEntry entry, List<Object> items);
}
