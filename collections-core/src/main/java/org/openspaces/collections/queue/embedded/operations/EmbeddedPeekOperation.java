package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;

import java.util.List;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.ITEMS_PATH;

/**
 * @author Svitlana_Pogrebna
 */
public class EmbeddedPeekOperation extends SpaceEntriesAggregator<SerializableResult<Object>> {

    private static final long serialVersionUID = 1L;

    private transient SerializableResult<Object> result;

    @Override
    public String getDefaultAlias() {
        return "peek";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        final List<Object> items = (List<Object>) context.getPathValue(ITEMS_PATH);
        result = new SerializableResult<Object>(items.isEmpty() ? null : items.get(0));
    }

    @Override
    public SerializableResult<Object> getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(SerializableResult<Object> partitionResult) {
        this.result = partitionResult;
    }
}
