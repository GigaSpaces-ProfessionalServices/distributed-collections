package org.openspaces.collections.queue.distributed.data;

import org.openspaces.collections.queue.QueueMetadata;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Oleksiy_Dyagilev
 */
@SpaceClass
public class DistrQueueMetadata implements QueueMetadata {

    /** path is used in change api **/
    public static final String HEAD_PATH = "head";
    public static final String TAIL_PATH = "tail";
    public static final String BOUNDED_PATH = "bounded";
    public static final String CAPACITY_PATH = "capacity";
    public static final String REMOVED_INDEXES_PATH = "removedIndexes";
    public static final String REMOVED_INDEXES_SIZE_PATH = "removedIndexesSize";

    private String name;
    private Long head;
    private Long tail;
    private Boolean bounded;
    private Integer capacity;
    private Set<Long> removedIndexes;
    private Integer removedIndexesSize;

    public DistrQueueMetadata() {
    }

    public DistrQueueMetadata(String name, Long head, Long tail, Boolean bounded, Integer capacity) {
        this.name = name;
        this.head = head;
        this.tail = tail;
        this.bounded = bounded;
        this.capacity = capacity;
        this.removedIndexes = new HashSet<>();
        this.removedIndexesSize = 0;
    }

    @SpaceId
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getHead() {
        return head;
    }

    public void setHead(Long head) {
        this.head = head;
    }

    public Long getTail() {
        return tail;
    }

    public void setTail(Long tail) {
        this.tail = tail;
    }

    public Boolean getBounded() {
        return bounded;
    }

    public void setBounded(Boolean bounded) {
        this.bounded = bounded;
    }

    public Integer getCapacity() {
        return capacity;
    }

    public void setCapacity(Integer capacity) {
        this.capacity = capacity;
    }

    public Set<Long> getRemovedIndexes() {
        return removedIndexes;
    }

    public void setRemovedIndexes(Set<Long> removedIndexes) {
        this.removedIndexes = removedIndexes;
    }

    public Integer getRemovedIndexesSize() {
        return removedIndexesSize;
    }

    public void setRemovedIndexesSize(Integer removedIndexesSize) {
        this.removedIndexesSize = removedIndexesSize;
    }

    @Override
    public String toString() {
        return "DistrQueueMetadata{" +
                "name='" + name + '\'' +
                ", head=" + head +
                ", tail=" + tail +
                ", bounded=" + bounded +
                ", capacity=" + capacity +
                ", removedIndexes=" + removedIndexes +
                ", removedIndexesSize=" + removedIndexesSize +
                '}';
    }

    @Override
    public Integer getSize() {
        return (int) (getTail() - getHead() - getRemovedIndexesSize());
    }
}
