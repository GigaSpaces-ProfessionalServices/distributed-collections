package org.openspaces.collections.queue.embedded.operations;

import com.gigaspaces.client.ChangeOperationResult;
import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

import java.util.List;
import java.util.Objects;

import org.openspaces.collections.exception.GigaBlockingQueueCapcityReachedException;
import org.openspaces.collections.util.CollectionUtils;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.CAPACITY_PATH;
import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.ITEMS_PATH;
import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.SIZE_PATH;

/**
 * @author Michael Raney
 */
public class EmbeddedOfferOperation extends CustomChangeOperation {

	private static final long serialVersionUID = 1L;

	private final Object element;

	public EmbeddedOfferOperation(Object element) {
		this.element = Objects.requireNonNull(element);
	}

	@Override
    public ChangeOperationResult getChangeOperationResult(java.io.Serializable result) {
		return super.getChangeOperationResult(result);
	}
	
	@Override
	public String getName() {
		return "offer";
	}
	
	@Override
	public Object change(MutableServerEntry entry) {
		List<Object> originalQueue = (List<Object>) entry.getPathValue(ITEMS_PATH);
		final Integer capacity = (Integer) entry.getPathValue(CAPACITY_PATH);
		final int originalSize = originalQueue.size();
		
		if (isSpaceAvailible(capacity, originalSize)) {
			final List<Object> items = CollectionUtils.cloneCollection(originalQueue);
			items.add(element);
			entry.setPathValue(SIZE_PATH, items.size());
			entry.setPathValue(ITEMS_PATH, items);
			return null;
		} else {
			throw new GigaBlockingQueueCapcityReachedException("Queue full capacity:"+capacity+" originalSize:"+originalSize);
		}
	}
	
	private boolean isSpaceAvailible(Integer capacity, int size) {
		return capacity == null || capacity >= size + 1;
	}
}
