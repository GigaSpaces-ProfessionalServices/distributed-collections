package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;

import java.util.List;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.ITEMS_PATH;

/**
 * @author Svitlana_Pogrebna
 */
public class EmbeddedPeekOperation extends SpaceEntriesAggregator<SerializableResult<byte[]>> {

    private static final long serialVersionUID = 1L;

    private transient SerializableResult<byte[]> result;

    @Override
    public String getDefaultAlias() {
        return "peek";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        final List<byte[]> items = (List<byte[]>) context.getPathValue(ITEMS_PATH);
        result = new SerializableResult<byte[]>(items.isEmpty() ? null : items.get(0));
    }

    @Override
    public SerializableResult<byte[]> getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(SerializableResult<byte[]> partitionResult) {
        this.result = partitionResult;
    }
}
