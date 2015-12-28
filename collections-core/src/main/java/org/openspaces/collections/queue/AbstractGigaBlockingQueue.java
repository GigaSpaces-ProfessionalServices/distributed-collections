/**
 * 
 */
package org.openspaces.collections.queue;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Objects;

import org.openspaces.core.GigaSpace;

/**
 * @author Svitlana_Pogrebna
 */
public abstract class AbstractGigaBlockingQueue<E> extends AbstractQueue<E> implements GigaBlockingQueue<E> {

    protected final GigaSpace space;
    protected final String queueName;
    protected final boolean bounded;
    protected final int capacity;

    /**
     * Creates blocking queue
     *
     * @param space           giga space
     * @param queueName       unique queue queueName
     * @param capacity        queue capacity
     * @param bounded         flag whether queue is bounded
     */
    public AbstractGigaBlockingQueue(GigaSpace space, String queueName, int capacity, boolean bounded) {
        if (queueName == null || queueName.isEmpty()) {
            throw new IllegalArgumentException("'queueName' parameter must not be null or empty");
        }
        if (capacity < 0) {
            throw new IllegalArgumentException("'capacity' parameter must not be negative");
        }
        this.space = Objects.requireNonNull(space, "'space' parameter must not be null");
        this.queueName = queueName;
        this.capacity = capacity;
        this.bounded = bounded;

        createNewIfRequired();
    }
    
    @Override
    public String getName() {
        return queueName;
    }
    
    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        checkNotNull(c);
        if (c == this) {
            throw new IllegalArgumentException();
        }
        if (maxElements <= 0) {
            return 0;
        }

        int max = Math.min(size(), maxElements);

        for (int i = 0; i < max; i++) {
            E element = poll();
            c.add(element);
        }

        return max;
    }
    
    @Override
    public boolean removeAll(Collection<?> c) {
        return super.removeAll(Objects.requireNonNull(c, "Collection parameter must not be null"));
    }
    
    @Override
    public boolean retainAll(Collection<?> c) {
        return super.retainAll(Objects.requireNonNull(c, "Collection parameter must not be null"));
    }
    
    protected abstract void createNewIfRequired();
    
    /**
     * Throws NullPointerException if argument is null.
     *
     * @param v the element
     */
    protected static void checkNotNull(Object v) {
        if (v == null) {
            throw new NullPointerException();
        }
    }
}
