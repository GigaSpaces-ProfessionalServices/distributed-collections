package org.openspaces.collections.queue.embedded.operations;

import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.ITEMS_PATH;
import static org.openspaces.collections.queue.embedded.data.EmbeddedQueueContainer.SIZE_PATH;

import java.util.List;

import org.openspaces.collections.util.CollectionUtils;

import com.gigaspaces.client.CustomChangeOperation;
import com.gigaspaces.server.MutableServerEntry;

public class EmbeddedClearOperation extends CustomChangeOperation {

	private static final long serialVersionUID = 1L;

	@Override
	public String getName() {
		return "clearQueue";
	}

	//The two main reasons are transactions and visibility.
	//Transactions - if the change is part of a transaction, and you modify the original list, the space cannot roll it back if the transaction is later aborted.
	//Visibility - If the list is not cloned, the change is not guaranteed to be visible to other threads after the change is completed.
	@Override
	public Object change(MutableServerEntry entry) {
		List<Object> originalList = (List<Object>) entry.getPathValue(ITEMS_PATH);
		List<Object> clonedList = CollectionUtils.cloneCollection(originalList);
		
		clonedList.clear();
		
		entry.setPathValue(SIZE_PATH, clonedList.size());
		entry.setPathValue(ITEMS_PATH, clonedList);
		
		return null;
	}

}
