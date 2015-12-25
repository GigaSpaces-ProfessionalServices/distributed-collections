package org.openspaces.collections.queue.data;

import java.util.Objects;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;

/**
 * @author Oleksiy_Dyagilev
 */
@SpaceClass
public class QueueItem<T> {

    private QueueItemKey itemKey;
    private Integer routing;
    private T item;
    
    public QueueItem() {
    }

    public QueueItem(QueueItemKey itemKey, T item, Integer routing) {
        this.itemKey = Objects.requireNonNull(itemKey, "'itemKey' parameter must not be null");
        this.routing = Objects.requireNonNull(routing, "'routing' parameter must not be null");
        this.item = item;
    }

    @SpaceId
    public QueueItemKey getItemKey() {
        return itemKey;
    }

    public void setItemKey(QueueItemKey itemKey) {
        this.itemKey = itemKey;
    }

    @SpaceRouting
    public Integer getRouting() {
        return routing;
    }

    public void setRouting(Integer routing) {
        this.routing = routing;
    }
    
    public T getItem() {
        return item;
    }

    public void setItem(T item) {
        this.item = item;
    }
}
