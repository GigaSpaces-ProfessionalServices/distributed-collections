package org.openspaces.collections.queue.distributed.operations;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;


import org.openspaces.collections.exception.GigaBlockingQueueCapcityReachedException;

import static org.openspaces.collections.queue.distributed.data.DistrQueueMetadata.*;
/**
 * @author Michael Raney
 */
public class DistrOfferOperation extends CustomChangeOperation {

    
    public DistrOfferOperation() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object change(MutableServerEntry entry) {
        Long tail = (Long) entry.getPathValue(TAIL_PATH);
        Long head = (Long) entry.getPathValue(HEAD_PATH);
        boolean bounded = (Boolean) entry.getPathValue(BOUNDED_PATH);
        int capacity = (Integer) entry.getPathValue(CAPACITY_PATH);
        int removedIndexesSize = (Integer) entry.getPathValue(REMOVED_INDEXES_SIZE_PATH);

        if (hasSpaceAvailable(bounded, tail, head, capacity, removedIndexesSize)) {
            long newTail = tail + 1;
            entry.setPathValue(TAIL_PATH, newTail);
            return newTail;
        } else {
            throw new GigaBlockingQueueCapcityReachedException("Queue full");
        }
    }

    private boolean hasSpaceAvailable(boolean bounded, Long tail, Long head, int capacity, int removedIndexesSize) {
        return !bounded || (capacity >= (tail - head + 1 - removedIndexesSize));
    }

    @Override
    public String getName() {
        return "offer";
    }
}
