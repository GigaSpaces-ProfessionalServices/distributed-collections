package org.openspaces.collections.queue.operations;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;
import org.openspaces.collections.queue.data.QueueData;

/**
 * @author Oleksiy_Dyagilev
 */
public class OfferOperation extends CustomChangeOperation {

    private final int itemsNumber;

    public OfferOperation(int itemsNumber) {
        this.itemsNumber = itemsNumber;
    }


    @Override
    public Object change(MutableServerEntry entry) {
        String tailPath = "tail";

        Long currTail = (Long)entry.getPathValue(tailPath);

        long newTail;
        if (currTail == null) {
            newTail = (long) itemsNumber;
        } else {
            newTail = currTail + itemsNumber;
        }

        entry.setPathValue(tailPath, newTail);;

        return newTail;
    }

    @Override
    public String getName() {
        return "offer";
    }
}
