/**
 * 
 */
package org.openspaces.collections.queue;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.openspaces.collections.CollocationMode;
import org.openspaces.core.GigaSpace;

/**
 * @author Svitlana_Pogrebna
 *
 */
public class EmbeddedGigaBlockingQueue<E> extends AbstractGigaBlockingQueue<E> {

    public EmbeddedGigaBlockingQueue(GigaSpace space, String queueName, int capacity, boolean bounded) {
        super(space, queueName, capacity, bounded);
    }

    @Override
    public boolean offer(E e) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void put(E e) throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public E take() throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public int remainingCapacity() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public E poll() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public E peek() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public CollocationMode getCollocationMode() {
        return CollocationMode.EMBEDDED;
    }

    @Override
    public void close() throws Exception {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    protected void createNewIfRequired() {
        // TODO Auto-generated method stub
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
