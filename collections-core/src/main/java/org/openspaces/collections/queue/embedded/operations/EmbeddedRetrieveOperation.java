package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;

import java.util.ArrayList;
import java.util.List;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.ITEMS_PATH;

/**
 * @author Svitlana_Pogrebna
 *
 */
public class EmbeddedRetrieveOperation extends SpaceEntriesAggregator<SerializableResult<List<Object>>> {

    private static final long serialVersionUID = 1L;

    private transient SerializableResult<List<Object>> result;
    
    private final int index;
    private final int maxEntries;
    
    public EmbeddedRetrieveOperation(int index, int maxEntries) {
        if (index < 0) {
            throw new IndexOutOfBoundsException("'index' parameter must not be negative");
        }
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("'maxEntries' parameter must be positive");
        }
        this.index = index;
        this.maxEntries = maxEntries;
    }
    
    @Override
    public String getDefaultAlias() {
        return "retrieve";
    }
    
    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        @SuppressWarnings("unchecked")
        final List<Object> items = (List<Object>) context.getPathValue(ITEMS_PATH);
        final int size = items.size();
        
        if (index >= size) {
            result = new SerializableResult<>(null);
            return;
        }
        
        final int toIndex = index + maxEntries;
        final List<Object> subList = new ArrayList<>(items.subList(index, toIndex < size ? toIndex : size));
        result = new SerializableResult<>(subList);
    }

    @Override
    public SerializableResult<List<Object>> getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(SerializableResult<List<Object>> partitionResult) {
        this.result = partitionResult;
    }
}
