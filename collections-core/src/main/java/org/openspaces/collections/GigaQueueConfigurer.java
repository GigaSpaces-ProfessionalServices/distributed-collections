package org.openspaces.collections;

import org.openspaces.collections.queue.GigaBlockingQueue;
import org.openspaces.core.GigaSpace;

/**
 * @author Leonid_Poliakov
 */
public class GigaQueueConfigurer<T> {
    private final GigaQueueFactoryBean<T> gigaQueueFactoryBean = new GigaQueueFactoryBean<>();
    private GigaBlockingQueue<T> gigaQueue;

    public GigaQueueConfigurer(GigaSpace gigaSpace, String queueName, CollocationMode collocationMode) {
        gigaQueueFactoryBean.setGigaSpace(gigaSpace);
        gigaQueueFactoryBean.setQueueName(queueName);
        gigaQueueFactoryBean.setCollocationMode(collocationMode);
    }

    public GigaQueueConfigurer<T> capacity(Integer capacity) {
        gigaQueueFactoryBean.setCapacity(capacity);
        return this;
    }

    public GigaQueueConfigurer<T> elementType(Class<T> elementType) {
        gigaQueueFactoryBean.setElementType(elementType);
        return this;
    }

    public GigaBlockingQueue<T> gigaQueue() {
        if (gigaQueue == null) {
            gigaQueueFactoryBean.afterPropertiesSet();
            gigaQueue = gigaQueueFactoryBean.getObject();
        }

        return this.gigaQueue;
    }
}