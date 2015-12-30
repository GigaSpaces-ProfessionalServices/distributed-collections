/**
 * 
 */
package org.openspaces.collections.queue;

import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.gigaspaces.query.aggregators.AggregationSet;
import org.openspaces.collections.CollocationMode;
import org.openspaces.collections.queue.data.EmbeddedQueue;
import org.openspaces.collections.queue.data.EmbeddedQueueContainer;
import org.openspaces.collections.queue.operations.EmbeddedPeekOperation;
import org.openspaces.collections.queue.operations.EmbeddedPollOperation;
import org.openspaces.collections.queue.operations.EmbeddedQueueItemResult;
import org.openspaces.core.EntryAlreadyInSpaceException;
import org.openspaces.core.GigaSpace;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.openspaces.collections.queue.data.EmbeddedQueue.QUEUE_CONTAINER_PATH;
import static org.openspaces.collections.queue.data.EmbeddedQueueContainer.QUEUE_SIZE_PATH;
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
    public E poll() {
        final ChangeSet changeSet = new ChangeSet().custom(new EmbeddedPollOperation());
        
        final ChangeResult<EmbeddedQueue> changeResult = space.change(idQuery(), changeSet, ChangeModifiers.RETURN_DETAILED_RESULTS);
        
        final EmbeddedQueueItemResult result = toSingleResult(changeResult);
        //TODO: add deserialization 
        return (E) result.getItem();
    }

    @SuppressWarnings("unchecked")
    @Override
    public E peek() {
        final AggregationResult aggregationResult = space.aggregate(idQuery(), new AggregationSet().add(new EmbeddedPeekOperation()));
        
        final EmbeddedQueueItemResult result = toSingleResult(aggregationResult);
        //TODO: add deserialization 
        return (E) result.getItem();
    }

    @Override
    public CollocationMode getCollocationMode() {
        return CollocationMode.EMBEDDED;
    }

    @Override
    protected void createNewMetadataIfRequired() {
        try {
            //TODO: replace LinkedBlockingQueue with more efficient implementation
            Queue<Object> queue = bounded ? new LinkedBlockingQueue<Object>(capacity) : new LinkedBlockingQueue<Object>();
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
        final IdQuery<EmbeddedQueue> idQuery = idQuery().setProjections(QUEUE_CONTAINER_PATH + "." + QUEUE_SIZE_PATH);
        return space.read(idQuery).getContainer().getSize();
    }
    
    private IdQuery<EmbeddedQueue> idQuery() {
        return new IdQuery<EmbeddedQueue>(EmbeddedQueue.class, queueName);
    }
    
    @Override
    public void close() throws Exception {
        throw new UnsupportedOperationException("Not implemented yet");
    }

}
