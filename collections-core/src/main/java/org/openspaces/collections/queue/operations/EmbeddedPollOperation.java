package org.openspaces.collections.queue.operations;

import java.util.Queue;

/**
 * @author Svitlana_Pogrebna
 *
 */
public class EmbeddedPollOperation extends EmbeddedChangeOperation<Object> {

    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "poll";
    }

    @Override
    protected Object change(Queue<Object> queue) {
        return queue.poll();
    }
}
