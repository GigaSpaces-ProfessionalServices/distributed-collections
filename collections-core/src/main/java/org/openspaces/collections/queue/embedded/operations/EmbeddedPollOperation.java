package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.server.MutableServerEntry;

import java.util.List;

/**
 * @author Svitlana_Pogrebna
 */
public class EmbeddedPollOperation extends EmbeddedChangeOperation<byte[]> {

    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "poll";
    }

    @Override
    protected byte[] change(MutableServerEntry entry, List<byte[]> items) {
        return items.isEmpty() ? null : items.remove(0);
    }
}
