package org.openspaces.collections.queue.data;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;

/**
 * @author Oleksiy_Dyagilev
 */
@SpaceClass
public class QueueItem<T> {

    private QueueItemKey itemKey;
    private T item;

    public QueueItem() {
    }

    public QueueItem(QueueItemKey itemKey, T item) {
        this.itemKey = itemKey;
        this.item = item;
    }

    @SpaceId
    public QueueItemKey getItemKey() {
        return itemKey;
    }

    public void setItemKey(QueueItemKey itemKey) {
        this.itemKey = itemKey;
    }

    public T getItem() {
        return item;
    }

    public void setItem(T item) {
        this.item = item;
    }
}
