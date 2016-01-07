package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.server.MutableServerEntry;

import java.util.List;

/**
 * @author Svitlana_Pogrebna
 */
public class EmbeddedPollOperation extends EmbeddedChangeOperation<Object> {

    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "poll";
    }

    @Override
    protected Object change(MutableServerEntry entry, List<Object> items) {
        return items.isEmpty() ? null : items.remove(0);
    }
}
