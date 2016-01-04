/**
 *
 */
package org.openspaces.collections.queue.embedded.data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Objects;

import static org.openspaces.collections.util.SerializationUtils.readNullableObject;
import static org.openspaces.collections.util.SerializationUtils.writeNullableObject;
/**
 * @author Svitlana_Pogrebna
 */
public class EmbeddedQueueContainer implements Externalizable {

    public static final String ITEMS_PATH = "items";
    public static final String SIZE_PATH = "size";
    public static final String CAPACITY_PATH = "capacity";

    private List<Object> items;
    private Integer size;
    private Integer capacity;

    public EmbeddedQueueContainer() {
    }

    public EmbeddedQueueContainer(List<Object> items, Integer capacity) {
        this.items = Objects.requireNonNull(items, "'items' parameter must not be null");
        this.size = items.size();
        this.capacity = capacity;
    }

    public List<Object> getItems() {
        return items;
    }

    public void setItems(List<Object> list) {
        this.items = list;
    }

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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(getItems());
        out.writeInt(getSize());
        writeNullableObject(out, getCapacity());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setItems((List<Object>)in.readObject());
        setSize(in.readInt());
        setCapacity((Integer)readNullableObject(in));
    }
}
