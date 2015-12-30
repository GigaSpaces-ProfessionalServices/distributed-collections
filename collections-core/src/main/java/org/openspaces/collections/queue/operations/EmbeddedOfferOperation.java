/**
 * 
 */
package org.openspaces.collections.queue.operations;

import java.util.Objects;
import java.util.Queue;

/**
 * @author Svitlana_Pogrebna
 *
 */
public class EmbeddedOfferOperation extends EmbeddedChangeOperation<Boolean> {

    private static final long serialVersionUID = 1L;
   
    private final Object element;
    
    public EmbeddedOfferOperation(Object element) {
        this.element = Objects.requireNonNull(element);
    }
    
    @Override
    public String getName() {
        return "offer";
    }

    @Override
    protected Boolean change(Queue<Object> queue) {
        return queue.offer(element);
    }
}
