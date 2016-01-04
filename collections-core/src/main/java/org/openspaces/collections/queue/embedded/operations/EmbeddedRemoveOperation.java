package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.server.MutableServerEntry;

import java.util.List;

/**
 * @author Svitlana_Pogrebna
 */
public class EmbeddedRemoveOperation extends EmbeddedChangeOperation<Boolean> {

    private static final long serialVersionUID = 1L;

    private final Object item;
    
    public EmbeddedRemoveOperation(Object item) {
        this.item = item;
    }
    
    @Override
    public String getName() {
        return "remove";
    }

    @Override
    protected Boolean change(MutableServerEntry entry, List<Object> items) {
        return items.remove(item);
    }
}
