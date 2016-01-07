package org.openspaces.collections.queue.embedded.data;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import org.openspaces.collections.queue.QueueMetadata;

import java.util.List;
import java.util.Objects;

/**
 * The space class that holds queue metadata and items
 *
 * @author Svitlana_Pogrebna
 */
@SpaceClass
public class EmbeddedQueueContainer implements QueueMetadata {

    public static final String QUEUE_NAME_PATH = "name";
    public static final String ITEMS_PATH = "items";
    public static final String SIZE_PATH = "size";
    public static final String CAPACITY_PATH = "capacity";

    private String name;
    private List<Object> items;
    private Integer size;
    private Integer capacity;

    public EmbeddedQueueContainer() {
    }

    public EmbeddedQueueContainer(String name, List<Object> items, Integer capacity) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("'name' parameter must not be null or empty");
        }
        this.name = name;
        this.items = Objects.requireNonNull(items, "'items' parameter must not be null");
        this.size = items.size();
        this.capacity = capacity;
    }

    @SpaceId
    @SpaceRouting
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public List<Object> getItems() {
        return items;
    }

    public void setItems(List<Object> list) {
        this.items = list;
    }

    @Override
    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Integer getCapacity() {
        return capacity;
    }

    public void setCapacity(Integer capacity) {
        this.capacity = capacity;
    }
}
