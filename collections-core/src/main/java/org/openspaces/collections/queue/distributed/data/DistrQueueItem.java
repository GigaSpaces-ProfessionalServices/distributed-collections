package org.openspaces.collections.queue.distributed.data;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;

import java.util.Objects;

/**
 * @author Oleksiy_Dyagilev
 */
@SpaceClass
public class DistrQueueItem {

    private DistrQueueItemKey itemKey;
    private Integer routing;
    private Object item;

    public DistrQueueItem() {
    }

    public DistrQueueItem(DistrQueueItemKey itemKey, Object item, Integer routing) {
        this.itemKey = Objects.requireNonNull(itemKey, "'itemKey' parameter must not be null");
        this.routing = Objects.requireNonNull(routing, "'routing' parameter must not be null");
        this.item = item;
    }

    @SpaceId
    public DistrQueueItemKey getItemKey() {
        return itemKey;
    }

    public void setItemKey(DistrQueueItemKey itemKey) {
        this.itemKey = itemKey;
    }

    @SpaceRouting
    public Integer getRouting() {
        return routing;
    }

    public void setRouting(Integer routing) {
        this.routing = routing;
    }

    public Object getItem() {
        return item;
    }

    public void setItem(Object item) {
        this.item = item;
    }
}
