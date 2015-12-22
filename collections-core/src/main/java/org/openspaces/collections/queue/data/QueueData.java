package org.openspaces.collections.queue.data;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;

/**
 * TODO: think about better name
 *
 * @author Oleksiy_Dyagilev
 */
@SpaceClass
public class QueueData {

    private String name;
    private Long head;
    private Long tail;
    private Boolean bounded;
    private Integer capacity;

    public QueueData() {
    }

    public QueueData(String name, Long head, Long tail, Boolean bounded, Integer capacity) {
        this.name = name;
        this.head = head;
        this.tail = tail;
        this.bounded = bounded;
        this.capacity = capacity;
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
}
