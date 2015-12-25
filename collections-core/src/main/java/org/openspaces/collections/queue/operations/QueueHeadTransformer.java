package org.openspaces.collections.queue.operations;

import java.util.Set;

/**
 * A class for managing queue head 
 * 
 * @author Svitlana_Pogrebna
 */
public class QueueHeadTransformer {

    /**
     * Moves forward the queue head and skips the removedIndexes. 
     * If it reaches the tail then empty result with tail index is returned.
     * 
     * @param head
     * @param tail
     * @param removedIndexes
     * @return queue head result.
     */
    public QueueHeadResult forwardQueueHead(long head, long tail, Set<Long> removedIndexes) {
        if (head == tail) {
            throw new IllegalArgumentException("Queue should not be empty");
        }

        // skip elements that were removed with iterator.remove()
        long nextNonRemovedIndex = nextNonRemovedIndex(head, removedIndexes);
        return nextNonRemovedIndex > tail ? QueueHeadResult.emptyQueueResult(tail) : QueueHeadResult.headIndexResult(nextNonRemovedIndex);
    }
    

    /**
     * returns next non removed index, as a side effect cleans up 'removedIndexes' set removing indexes we no longer need
     */
    private long nextNonRemovedIndex(final long index, Set<Long> removedIndexes) {
        long next = index + 1;
        while (removedIndexes.remove(next)) {
            next++;
        }
        return next;
    }
}
