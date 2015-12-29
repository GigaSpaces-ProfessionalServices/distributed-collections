/**
 * 
 */
package org.openspaces.collections.queue;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.queue.data.EmbeddedQueue;
import org.openspaces.collections.queue.data.EmbeddedQueueContainer;
import org.openspaces.core.EntryAlreadyInSpaceException;
import org.openspaces.core.GigaSpace;

import com.gigaspaces.client.WriteModifiers;

/**
 * @author Svitlana_Pogrebna
 *
 */
public class EmbeddedGigaBlockingQueue<E> extends AbstractGigaBlockingQueue<E> {

    public EmbeddedGigaBlockingQueue(GigaSpace space, String queueName) {
        super(space, queueName, 0, false);
    }
    
    public EmbeddedGigaBlockingQueue(GigaSpace space, String queueName, int capacity) {
        super(space, queueName, capacity, true);
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
    protected void createNewMetadataIfRequired() {
        try {
            //TODO: replace LinkedBlockingQueue with more efficient implementation
            Queue<byte[]> queue = bounded ? new LinkedBlockingQueue<byte[]>(capacity) : new LinkedBlockingQueue<byte[]>();
            EmbeddedQueue embeddedQueue = new EmbeddedQueue(queueName, new EmbeddedQueueContainer(queue));
            space.write(embeddedQueue, WriteModifiers.WRITE_ONLY);
        } catch (EntryAlreadyInSpaceException e) {
            // no-op
        }
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
