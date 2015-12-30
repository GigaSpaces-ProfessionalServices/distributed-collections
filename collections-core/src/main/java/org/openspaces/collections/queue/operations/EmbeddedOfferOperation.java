/**
 * 
 */
package org.openspaces.collections.queue.operations;

import com.gigaspaces.server.MutableServerEntry;

import java.util.List;
import java.util.Objects;

import static org.openspaces.collections.queue.data.EmbeddedQueue.QUEUE_CONTAINER_PATH;
import static org.openspaces.collections.queue.data.EmbeddedQueueContainer.CAPACITY_PATH;

/**
 * @author Svitlana_Pogrebna
 *
 */
public class EmbeddedOfferOperation extends EmbeddedChangeOperation<Boolean> {

    private static final long serialVersionUID = 1L;
   
    private final Object element;
    
    public EmbeddedOfferOperation(Object element) {
        this.element = Objects.requireNonNull(element);
    }
    
    @Override
    public String getName() {
        return "offer";
    }

    @Override
    protected Boolean change(MutableServerEntry entry, List<Object> items) {
        final Integer capacity = (Integer) entry.getPathValue(QUEUE_CONTAINER_PATH + "." + CAPACITY_PATH);
        return isSpaceAvailible(capacity, items.size()) ? items.add(element) : false;
    }
    
    private boolean isSpaceAvailible(Integer capacity, int size) {
        return capacity == null || capacity >= size + 1;
    }
}
