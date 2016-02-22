package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;

import java.io.Serializable;
import java.util.List;

import org.openspaces.collections.exception.GigaBlockingQueueElementNotFoundException;
import org.openspaces.collections.util.CollectionUtils;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.ITEMS_PATH;

/**
 * @author Svitlana_Pogrebna
 */
public class EmbeddedContaiinsOperation<E> extends SpaceEntriesAggregator<Boolean> {

    private static final long serialVersionUID = 1L;

    private transient Boolean result = false;
    
    private E item;
    
    public EmbeddedContaiinsOperation(E item) {
		this.item = item;
	}
    @Override
    public String getDefaultAlias() {
        return "contains";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        
    	final List<Object> items = (List<Object>) context.getPathValue(ITEMS_PATH);
        
    	int itemIndex = CollectionUtils.containsUsingDeepCompare(item, items);
		
    	result = itemIndex >= 0;
    }

    @Override
    public Boolean getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(Boolean partitionResult) {
        if(partitionResult == true)
        	result = true;
    }
}
