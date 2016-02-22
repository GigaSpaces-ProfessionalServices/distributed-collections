package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.ITEMS_PATH;
import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.SIZE_PATH;

import java.util.List;
import java.util.Objects;

import org.openspaces.collections.util.CollectionUtils;

/**
 * @author Svitlana_Pogrebna
 */
public class EmbeddedRemoveIteratorOperation extends CustomChangeOperation {

    private static final long serialVersionUID = 1L;

    private final int index;
    private final Object item;
    
    public EmbeddedRemoveIteratorOperation(int index, Object item) {
        if (index < 0) {
            throw new IndexOutOfBoundsException("'index' parameter must not be negative");
        }
        if (item == null) {
            throw new IllegalArgumentException("'item' must not be null");
        }
        this.index = index;
        this.item = item;
    }
    
    @Override
    public String getName() {
        return "remove";
    }

    @Override
    public Object change(MutableServerEntry entry) {
    	final List<Object> originalQueue = (List<Object>) entry.getPathValue(ITEMS_PATH);
        
        if (index < originalQueue.size() && Objects.deepEquals(item, originalQueue.get(index))) {
        	final List<Object> newitems = CollectionUtils.cloneCollection(originalQueue);
        	newitems.remove(index);
        	entry.setPathValue(SIZE_PATH, newitems.size());
            entry.setPathValue(ITEMS_PATH, newitems);
        }
        return null;
    }
}