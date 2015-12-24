package org.openspaces.collections.queue.operations;

import static org.openspaces.collections.queue.data.QueueData.HEAD_PATH;
import static org.openspaces.collections.queue.data.QueueData.REMOVED_INDEXES_PATH;
import static org.openspaces.collections.queue.data.QueueData.TAIL_PATH;

import java.util.HashSet;
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
        
        if (tail == head) {
            return QueueHeadResult.emptyQueueResult();
        }
        
        // skip elements that were removed with iterator.remove()
        final Set<Long> removedIndexes = new HashSet<>((Set<Long>) entry.getPathValue(REMOVED_INDEXES_PATH));
        final int removedIndexesOrigSize = removedIndexes.size();
       
        QueueHeadResult result = new QueueHeadTransformer().transform(head, tail, removedIndexes);
        entry.setPathValue(HEAD_PATH, result.getHeadIndex());

        // update removed indexes set if it was changed
        if (removedIndexes.size() != removedIndexesOrigSize) {
            entry.setPathValue(REMOVED_INDEXES_PATH, removedIndexes);
        }

        return result;
    }

    @Override
    public String getName() {
        return "poll";
    }
}