package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.ITEMS_PATH;
import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.SIZE_PATH;

import java.util.ArrayList;
import java.util.List;

import org.openspaces.collections.exception.GigaBlockingQueueCapcityReachedException;
import org.openspaces.collections.exception.GigaBlockingQueueElementNotFoundException;
import org.openspaces.collections.util.CollectionUtils;

/**
 * @author Michael Raney
 */
public class EmbeddedRemoveOperation extends CustomChangeOperation {

    private static final long serialVersionUID = 1L;

    private final Object element;
    
    public EmbeddedRemoveOperation(Object element) {
        if (element == null) {
            throw new IllegalArgumentException("'item' must not be null");
        }
        
        this.element = element;
    }
    
    @Override
    public String getName() {
        return "remove";
    }

    public Object change(MutableServerEntry entry) {
    	List<Object> originalQueue = (List<Object>) entry.getPathValue(ITEMS_PATH);
		
		final int itemIndex = CollectionUtils.containsUsingDeepCompare(element, originalQueue);
		
		if(itemIndex >= 0){
			 final List<Object> items = CollectionUtils.cloneCollection(originalQueue);
			 items.remove(itemIndex);
		  	 entry.setPathValue(SIZE_PATH, items.size());
		     entry.setPathValue(ITEMS_PATH, items);
			  return true;
	      }else {
			throw new GigaBlockingQueueElementNotFoundException("Item not found");
		}
	}
}
