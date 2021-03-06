package org.openspaces.collections.queue;

import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.j_spaces.core.client.SQLQuery;

import org.openspaces.collections.serialization.ElementSerializer;
import org.openspaces.collections.util.MiscUtils;
import org.openspaces.core.GigaSpace;

import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The class provides implementation of some blocking queue operations
 *
 * @author Svitlana_Pogrebna
 */
public abstract class AbstractGigaBlockingQueue<E, M extends QueueMetadata> extends AbstractQueue<E>
		implements GigaBlockingQueue<E> {

	protected static final String NULL_ELEMENT_ERR_MSG = "Queue doesn't support null elements";

	protected static final String QUEUE_SIZE_CHANGE_LISTENER_THREAD_NAME = "Queue size change listener - ";
	private static final long SIZE_CHANGE_LISTENER_TIMEOUT_MS = 5000;
	
	private static final int DEFAULT_BULK_WRITE_SIZE = 500;
	private static final double PERCENT_WRITES_PER_TOTAL_LOCKS = 0.20;

	protected final GigaSpace space;
	protected final String queueName;
	protected final boolean bounded;
	protected final int capacity;
	protected final ElementSerializer<E> serializer;

	protected final Semaphore readSemaphore;
	protected final Semaphore writeSemaphore;

	protected final Thread sizeChangeListenerThread;

	protected volatile boolean queueClosed;
	private final Integer bulkWriteAmount;

	/**
	 * Creates blocking queue
	 *
	 * @param space
	 *            giga space
	 * @param queueName
	 *            unique queue queueName
	 * @param capacity
	 *            queue capacity
	 * @param bounded
	 *            flag whether queue is bounded
	 * @param serializer
	 *            element serializer/deserializer
	 */
	public AbstractGigaBlockingQueue(GigaSpace space, String queueName, int capacity, boolean bounded,
			ElementSerializer<E> serializer) {
		
		if (queueName == null || queueName.isEmpty()) {
			throw new IllegalArgumentException("'queueName' parameter must not be null or empty");
		}
		if (capacity < 0) {
			throw new IllegalArgumentException("'capacity' parameter must not be negative");
		}
		if (serializer == null) {
			throw new IllegalArgumentException("'serializer' parameter must not be null");
		}
		
		this.space = requireNonNull(space, "'space' parameter must not be null");
		this.queueName = queueName;
		this.capacity = capacity;
		this.bounded = bounded;
		this.serializer = serializer;

		final M queueMetadata = getOrCreate();
		final int size = queueMetadata.getSize();
		this.bulkWriteAmount = calculateBulkWriteSize(bounded, capacity);
		System.out.println("bulkWriteAmount:" + bulkWriteAmount);
		this.readSemaphore = new Semaphore(size, true);
		this.writeSemaphore = bounded ? new Semaphore(capacity - size) : null;

		// start size change listener thread
		final Runnable sizeChangeListener = createSizeChangeListener(queueMetadata);
		this.sizeChangeListenerThread = new Thread(sizeChangeListener,
				QUEUE_SIZE_CHANGE_LISTENER_THREAD_NAME + queueName);
		this.sizeChangeListenerThread.start();
	}

	// For bulk writes in bounded queue we don't want to consume all locks
	private static int calculateBulkWriteSize(boolean bounded, int capacity) {
		if (!bounded || capacity == 0) {
			return DEFAULT_BULK_WRITE_SIZE;
		}
		if (capacity * PERCENT_WRITES_PER_TOTAL_LOCKS >= DEFAULT_BULK_WRITE_SIZE) {
			return DEFAULT_BULK_WRITE_SIZE;
		} else {
			return (int) (capacity * PERCENT_WRITES_PER_TOTAL_LOCKS);
		}
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
		requireNonNull(c, "Collection parameter must not be null");
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
		requireNonNull(c, "Collection parameter must not be null");
		return super.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		requireNonNull(c, "Collection parameter must not be null");
		return super.retainAll(c);
	}

	@Override
	public int remainingCapacity() {
		if (!bounded) {
			return Integer.MAX_VALUE;
		}

		return capacity - size();
	}
	
	@Override
	public boolean addAll(Collection<? extends E> c) {
		requireNonNull(c, "Collection parameter must not be null");

		if (c == this)
			throw new IllegalArgumentException();

		int writeBufferSize = c.size();
		
		List<Object> writeBuffer = new ArrayList<Object>((writeBufferSize > bulkWriteAmount) ? bulkWriteAmount : writeBufferSize);

		Iterator<? extends E> inputIterator = c.iterator();

		boolean modified = false;
		
		while (inputIterator.hasNext()) {
			E oneItem = inputIterator.next();

			if (oneItem == null) 
				throw new NullPointerException("Item in collection is null");
			
			writeBuffer.add(serialize(oneItem));

			int currentBufferSize = writeBuffer.size();
			
			if (currentBufferSize >= bulkWriteAmount || !inputIterator.hasNext()) {
				if (bounded) {
					//TODO:Aquire the entire size of the input collection?
					boolean aquiredPermits = writeSemaphore.tryAcquire(currentBufferSize);
					if (!aquiredPermits) 
						throw new IllegalStateException("Queue full");
				}
				
				if (writeMultipleEntities(writeBuffer))
					modified = true;
				else
					throw new IllegalStateException("Queue full");
			
				writeBuffer.clear();
			}
		}
		return modified;
	}

	/**
	 * Writes multiple items to the queue
	 * 
	 * @return queue metadata
	 */
	protected abstract boolean writeMultipleEntities(List<Object> c);

	/**
	 * Writes item to the queue
	 * 
	 * @return queue metadata
	 */
	protected abstract boolean writeEntity(E element);

	@Override
	public void put(E element) throws InterruptedException {
		requireNonNull(element, "Element parameter must not be null");

		if (!bounded) {
			writeEntity(element);
			return;
		}

		while (true) {
			writeSemaphore.acquire();
			if (writeEntity(element)) {
				return;
			}
		}
	}

	@Override
	public boolean offer(E element) {
		requireNonNull(element, "Element parameter must not be null");

		if (!bounded) {
			return writeEntity(element);
		}

		boolean aquired = writeSemaphore.tryAcquire(1);
		if (aquired) {
			return writeEntity(element);
		} else {
			return false;
		}
	}

	@Override
	public boolean offer(E element, long timeout, TimeUnit unit) throws InterruptedException {
		requireNonNull(element, "Element parameter must not be null");

		if (!bounded) {
			return writeEntity(element);
		}

		long endTime = currentTimeMillis() + MILLISECONDS.convert(timeout, unit);

		while (currentTimeMillis() < endTime) {
			if (writeSemaphore.tryAcquire(timeout, unit)) {
				if (writeEntity(element)) {
					return true;
				}
			}
		}

		return false;
	}

	@Override
	public E take() throws InterruptedException {

		while (true) {
			readSemaphore.acquire();
			E e = poll();
			if (e != null) {
				return e;
			}
		}
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		long endTime = currentTimeMillis() + MILLISECONDS.convert(timeout, unit);

		while (currentTimeMillis() < endTime) {
			if (readSemaphore.tryAcquire(timeout, unit)) {
				E e = poll();
				if (e != null) {
					return e;
				}
			}
		}
		return null;
	}

	/**
	 * Creates new queue metadata if it has not been found by queue name
	 * 
	 * @return queue metadata
	 */
	protected abstract M getOrCreate();

	/**
	 * Creates SizeChangeListener based on queue metadata
	 * 
	 * @param queueMetadata
	 *            queue metadata
	 */
	protected abstract AbstractSizeChangeListener createSizeChangeListener(M queueMetadata);

	/**
	 * Set the proper number of permits for 'read' and 'write' semaphore based
	 * on the current queue size
	 */
	protected void onSizeChanged(int size) {
		readSemaphore.drainPermits();
		if (size > 0) {
			readSemaphore.release(size);
		}

		if (bounded) {
			writeSemaphore.drainPermits();
			int availableSpace = capacity - size;
			if (availableSpace > 0) {
				writeSemaphore.release(availableSpace);
			}
		}
	}

	/**
	 * extract single result from the aggregation result
	 */
	@SuppressWarnings("unchecked")
	protected <T extends Serializable> T toSingleResult(AggregationResult aggregationResult) {
		if (aggregationResult.size() == 0 && queueClosed) {
			throw new IllegalStateException("Queue has been closed(deleted) from the grid: " + queueName);
		} else if (aggregationResult.size() != 1) {
			throw new IllegalStateException("Unexpected aggregation result size: " + aggregationResult.size());
		}

		return (T) aggregationResult.get(0);
	}

	/**
	 * extract single result from the generic change api result
	 */
	@SuppressWarnings("unchecked")
	protected <T extends Serializable> T toSingleResult(ChangeResult<?> changeResult) {
		if (changeResult.getNumberOfChangedEntries() == 0 && queueClosed) {
			throw new IllegalStateException("Queue has been destroyed(deleted from the grid): " + queueName);
		} else if (changeResult.getNumberOfChangedEntries() > 1) {
			throw new IllegalStateException(
					"Unexpected number of changed entries: " + changeResult.getNumberOfChangedEntries());
		}

		return (T) changeResult.getResults().iterator().next().getChangeOperationsResults().iterator().next()
				.getResult();
	}

	protected Long getTailFromResult(ChangeResult<?> changeResult) {
		if (changeResult.getNumberOfChangedEntries() == 0 && queueClosed) {
			throw new IllegalStateException("Queue has been destroyed(deleted from the grid): " + queueName);
		} else if (changeResult.getNumberOfChangedEntries() > 1) {
			throw new IllegalStateException(
					"Unexpected number of changed entries: " + changeResult.getNumberOfChangedEntries());
		}

		return (Long) changeResult.getResults().iterator().next().getChangeOperationsResults().iterator().next()
				.getResult();
	}

	protected void checkNotClosed() {
		if (queueClosed) {
			throw new IllegalStateException("Queue has been closed " + queueName);
		}
	}

	protected Object serialize(E element) {
		return serializer.serialize(element);
	}

	protected E deserialize(Object payload) {
		return serializer.deserialize(payload);
	}

	@Override
	public void close() throws Exception {
		this.queueClosed = true;
		this.sizeChangeListenerThread.interrupt();
	}

	@Override
	public void destroy() throws Exception {
		this.close();
	}

	/**
	 * The listener that observes queue size changes and calls
	 * {@link #onSizeChanged} callback method
	 */
	protected abstract class AbstractSizeChangeListener implements Runnable {

		protected final M queueMetadata;

		public AbstractSizeChangeListener(M queueMetadata) {
			this.queueMetadata = queueMetadata;
		}

		@Override
		public void run() {
			final SQLQuery<M> query = query();

			while (!queueClosed) {

				populateParams(query);
				try {
					M foundMetadata = space.read(query, SIZE_CHANGE_LISTENER_TIMEOUT_MS);
					if (foundMetadata != null) {
						onSizeChanged(foundMetadata.getSize());
					}
				} catch (Exception e) {
					if (isInterrupted(e) || isClosedResource(e)) {
						return;
					} else {
						if (!queueClosed) {
							throw e;
						}
					}
				}
			}
		}

		/**
		 * Returns query that determines whether queue size is changed
		 * 
		 * @return
		 */
		protected abstract SQLQuery<M> query();

		/**
		 * Populates parameters to the size change query
		 * 
		 * @param query
		 */
		protected abstract void populateParams(SQLQuery<M> query);

		private boolean isInterrupted(Throwable e) {
			return MiscUtils.hasCause(e, InterruptedException.class);
		}

		private boolean isClosedResource(Throwable e) {
			return MiscUtils.hasCause(e, com.j_spaces.core.exception.ClosedResourceException.class);
		}
	}
}
