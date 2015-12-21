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
    private Integer capacity;

    public QueueData() {
    }

    public QueueData(String name, Long head, Long tail, Integer capacity) {
        this.name = name;
        this.head = head;
        this.tail = tail;
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

    public Integer getCapacity() {
        return capacity;
    }

    public void setCapacity(Integer capacity) {
        this.capacity = capacity;
    }
}
