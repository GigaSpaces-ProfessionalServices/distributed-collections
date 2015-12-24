package org.openspaces.collections.queue.operations;

import static org.openspaces.collections.queue.data.QueueData.BOUNDED_PATH;
import static org.openspaces.collections.queue.data.QueueData.CAPACITY_PATH;
import static org.openspaces.collections.queue.data.QueueData.HEAD_PATH;
import static org.openspaces.collections.queue.data.QueueData.TAIL_PATH;
import static org.openspaces.collections.util.CollectionUtils.checkIndexValid;
import static org.openspaces.collections.util.CollectionUtils.isQueueEmpty;

import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregatorContext;

public class PeekOperation extends SpaceEntriesAggregator<QueueHeadResult> {

    private static final long serialVersionUID = 1L;

    private transient QueueHeadResult result;
    
    @Override
    public String getDefaultAlias() {
        return "peek";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        Long tail = (Long) context.getPathValue(TAIL_PATH);
        Long head = (Long) context.getPathValue(HEAD_PATH);
        
        if (isQueueEmpty(head, tail)) {
            this.result = QueueHeadResult.emptyQueueResult();
        } else {
            boolean bounded = (Boolean) context.getPathValue(BOUNDED_PATH);
            int capacity = (Integer) context.getPathValue(CAPACITY_PATH);
            
            long itemIndex = head + 1;
            checkIndexValid(itemIndex, head, tail, bounded, capacity);
            
            this.result = QueueHeadResult.headIndexResult(itemIndex);
        }
    }

    @Override
    public QueueHeadResult getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(QueueHeadResult partitionResult) {
        this.result = partitionResult;
    }
}
