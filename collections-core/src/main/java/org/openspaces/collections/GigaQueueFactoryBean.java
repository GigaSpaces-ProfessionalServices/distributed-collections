package org.openspaces.collections;

import org.openspaces.collections.queue.DistributedGigaBlockingQueue;
import org.openspaces.collections.queue.GigaBlockingQueue;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * @author Leonid_Poliakov
 */
public class GigaQueueFactoryBean implements InitializingBean, DisposableBean, FactoryBean, BeanNameAware {
    private String beanName;
    private GigaSpace gigaSpace;
    private String queueName;
    private Integer capacity;
    private CollocationMode collocationMode;
    private GigaBlockingQueue gigaQueue;

    public void setGigaSpace(GigaSpace gigaSpace) {
        this.gigaSpace = gigaSpace;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void setCapacity(Integer capacity) {
        this.capacity = capacity;
    }

    public void setCollocationMode(CollocationMode collocationMode) {
        this.collocationMode = collocationMode;
    }

    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(gigaSpace, "gigaSpace property must be set");
        Assert.hasText(queueName, "queueName property must be set");
        Assert.notNull(collocationMode, "collocationMode property must be set");

        if (capacity != null) {
            gigaQueue = new DistributedGigaBlockingQueue(gigaSpace, queueName, capacity, collocationMode);
        } else {
            gigaQueue = new DistributedGigaBlockingQueue(gigaSpace, queueName, collocationMode);
        }
    }

    @Override
    public Object getObject() {
        return gigaQueue;
    }

    @Override
    public Class<?> getObjectType() {
        return gigaQueue == null ? GigaBlockingQueue.class : gigaQueue.getClass();
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void destroy() throws Exception {
    }
}