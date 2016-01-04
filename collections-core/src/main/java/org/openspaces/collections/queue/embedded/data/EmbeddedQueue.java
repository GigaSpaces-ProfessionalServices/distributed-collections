/**
 *
 */
package org.openspaces.collections.queue.embedded.data;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;

import java.util.Objects;

/**
 * @author Svitlana_Pogrebna
 */
@SpaceClass
public class EmbeddedQueue {

    public static final String QUEUE_NAME_PATH = "name";
    public static final String QUEUE_CONTAINER_PATH = "container";

    private String name;
    private EmbeddedQueueContainer container;

    public EmbeddedQueue() {
    }

    public EmbeddedQueue(String name, EmbeddedQueueContainer container) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("'name' parameter must not be null or empty");
        }
        this.name = name;
        this.container = Objects.requireNonNull(container, "'container' parameter must not be null");
    }

    @SpaceId
    @SpaceRouting
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public EmbeddedQueueContainer getContainer() {
        return container;
    }

    public void setContainer(EmbeddedQueueContainer container) {
        this.container = container;
    }
}