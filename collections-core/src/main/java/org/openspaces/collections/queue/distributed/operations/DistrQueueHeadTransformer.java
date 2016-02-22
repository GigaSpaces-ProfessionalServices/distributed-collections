package org.openspaces.collections.queue.distributed.operations;

import java.util.Set;

/**
 * A class for managing queue head
 *
 * @author Svitlana_Pogrebna
 */
public class DistrQueueHeadTransformer {

    /**
     * Moves forward the queue head and skips the removedIndexes.
     * If it reaches the tail then empty result with tail index is returned.
     *
     * @param head
     * @param tail
     * @param removedIndexes
     * @return queue head result.
     */
    public DistrQueueHeadResult forwardQueueHead(long head, long tail, Set<Object> removedIndexes) {
        if (head == tail) {
            throw new IllegalArgumentException("Queue should not be empty");
        }
        int sizeBefore = removedIndexes.size();

        // skip elements that were removed with iterator.remove()
        long nextNonRemovedIndex = nextNonRemovedIndex(head, removedIndexes);
        boolean changed = removedIndexes.size() != sizeBefore;
        return nextNonRemovedIndex > tail ? DistrQueueHeadResult.emptyQueueResult(tail, changed) : DistrQueueHeadResult.headIndexResult(nextNonRemovedIndex, changed);
    }


    /**
     * returns next non removed index, as a side effect cleans up 'removedIndexes' set removing indexes we no longer need
     */
    private long nextNonRemovedIndex(final long index, Set<Object> removedIndexes) {
        long next = index + 1;
        while (removedIndexes.remove(next)) {
            next++;
        }
        return next;
    }
}
