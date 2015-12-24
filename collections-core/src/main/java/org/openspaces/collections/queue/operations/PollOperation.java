package org.openspaces.collections.queue.operations;

import static org.openspaces.collections.queue.data.QueueData.HEAD_PATH;
import static org.openspaces.collections.queue.data.QueueData.REMOVED_INDEXES_PATH;
import static org.openspaces.collections.queue.data.QueueData.TAIL_PATH;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

/**
 * @author Oleksiy_Dyagilev
 */
public class PollOperation extends CustomChangeOperation {

    private static final long serialVersionUID = 1L;

    @Override
    @SuppressWarnings("unchecked")
    public Object change(MutableServerEntry entry) {
        final long tail = (long) entry.getPathValue(TAIL_PATH);
        final long head = (long) entry.getPathValue(HEAD_PATH);
        final Set<Long> removedIndexes = (Set<Long>) entry.getPathValue(REMOVED_INDEXES_PATH);
        final int removedIndexesOrigSize = removedIndexes.size();

        if (head == tail) {
            return QueueHeadResult.emptyQueueResult();
        }

        // skip elements that were removed with iterator,remove()
        long nextNonRemovedIndex = nextNonRemovedIndex(head, removedIndexes);

        long newHead;
        QueueHeadResult result;
        if (nextNonRemovedIndex > tail) {
            newHead = tail;
            result = QueueHeadResult.emptyQueueResult();
        } else {
            newHead = nextNonRemovedIndex;
            result = QueueHeadResult.headIndexResult(newHead);
        }

        entry.setPathValue(HEAD_PATH, newHead);

        // update removed indexes set if it was changed
        if (removedIndexes.size() != removedIndexesOrigSize) {
            entry.setPathValue(REMOVED_INDEXES_PATH, removedIndexes);
        }

        return result;
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


    @Override
    public String getName() {
        return "poll";
    }
}