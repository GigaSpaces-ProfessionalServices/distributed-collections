package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

import java.util.List;
import java.util.Objects;

import org.openspaces.collections.exception.GigaBlockingQueueCapcityReachedException;
import org.openspaces.collections.util.CollectionUtils;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.CAPACITY_PATH;
import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.SIZE_PATH;
import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.ITEMS_PATH;;

/**
 * @author Michael Raney
 */
public class EmbeddedOfferMultipleOperation extends CustomChangeOperation {

    private static final long serialVersionUID = 1L;

    private final List<Object> elements;

    public EmbeddedOfferMultipleOperation(List<Object> elements) {
        this.elements = Objects.requireNonNull(elements);
    }

    @Override
    public String getName() {
        return "offerMultiple";
    }

    @Override
    public Object change(MutableServerEntry entry) {
    	final List<Object> originalQueue = (List<Object>) entry.getPathValue(ITEMS_PATH);
        final Integer capacity = (Integer) entry.getPathValue(CAPACITY_PATH);
        
        final Integer currentSize = originalQueue.size();
        final Integer elementSize = elements.size();
        
        if(isSpacePartialAvailable(capacity, currentSize, elementSize)){
        	List<Object> newCollection = CollectionUtils.cloneCollection(originalQueue);
        	newCollection.addAll(elements);
        	entry.setPathValue(ITEMS_PATH, newCollection);
        	entry.setPathValue(SIZE_PATH, newCollection.size());
        	return null;
        }else{
        	throw new GigaBlockingQueueCapcityReachedException("Not enough empty space to add all items");
        }
    }
    private boolean isSpacePartialAvailable(Integer capacity, int currentSize, int totalItemsToAdd){
    	return capacity == null || capacity - currentSize - totalItemsToAdd >= 0;
    }
    
   
}
