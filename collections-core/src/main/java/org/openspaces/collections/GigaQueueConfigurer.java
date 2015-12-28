package org.openspaces.collections;

import org.openspaces.collections.queue.GigaBlockingQueue;
import org.openspaces.core.GigaSpace;

/**
 * @author Leonid_Poliakov
 */
public class GigaQueueConfigurer {
    private final GigaQueueFactoryBean gigaQueueFactoryBean = new GigaQueueFactoryBean();
    private GigaBlockingQueue gigaQueue;

    public GigaQueueConfigurer(GigaSpace gigaSpace, String queueName, CollocationMode collocationMode) {
        gigaQueueFactoryBean.setGigaSpace(gigaSpace);
        gigaQueueFactoryBean.setQueueName(queueName);
        gigaQueueFactoryBean.setCollocationMode(collocationMode);
    }

    public GigaQueueConfigurer capacity(Integer capacity) {
        gigaQueueFactoryBean.setCapacity(capacity);
        return this;
    }

    public GigaBlockingQueue gigaQueue() {
        if (gigaQueue == null) {
            gigaQueueFactoryBean.afterPropertiesSet();
            gigaQueue = (GigaBlockingQueue) gigaQueueFactoryBean.getObject();
        }

        return this.gigaQueue;
    }
}