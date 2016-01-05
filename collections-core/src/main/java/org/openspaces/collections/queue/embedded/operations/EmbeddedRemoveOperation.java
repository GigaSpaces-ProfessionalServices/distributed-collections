package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.server.MutableServerEntry;

import java.util.List;

/**
 * @author Svitlana_Pogrebna
 */
public class EmbeddedRemoveOperation extends EmbeddedChangeOperation<Boolean> {

    private static final long serialVersionUID = 1L;

    private final int index;
    private final Object item;
    
    public EmbeddedRemoveOperation(int index, Object item) {
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
    protected Boolean change(MutableServerEntry entry, List<Object> items) {
        if (index < items.size() && item.equals(items.get(index))) {
            items.remove(index);
            return true;
        }
        return false;
    }
}
