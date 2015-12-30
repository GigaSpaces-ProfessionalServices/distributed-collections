/**
 *
 */
package org.openspaces.collections.queue.operations;

import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;

import java.util.Queue;

import static org.openspaces.collections.queue.data.EmbeddedQueue.QUEUE_CONTAINER_PATH;
import static org.openspaces.collections.queue.data.EmbeddedQueueContainer.QUEUE_PATH;

/**
 * @author Svitlana_Pogrebna
 */
public class EmbeddedPeekOperation extends SpaceEntriesAggregator<EmbeddedQueueChangeResult<Object>> {

    private static final long serialVersionUID = 1L;

    private transient EmbeddedQueueChangeResult<Object> result;

    @Override
    public String getDefaultAlias() {
        return "peek";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        Queue<Object> queue = (Queue<Object>) context.getPathValue(QUEUE_CONTAINER_PATH + "." + QUEUE_PATH);
        result = new EmbeddedQueueChangeResult<Object>(queue.peek());
    }

    @Override
    public EmbeddedQueueChangeResult<Object> getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(EmbeddedQueueChangeResult<Object> partitionResult) {
        this.result = partitionResult;
    }
}
