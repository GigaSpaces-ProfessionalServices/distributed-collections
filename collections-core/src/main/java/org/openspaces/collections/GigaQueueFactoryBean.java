package org.openspaces.collections;

import org.openspaces.collections.queue.DistributedGigaBlockingQueue;
import org.openspaces.collections.queue.EmbeddedGigaBlockingQueue;
import org.openspaces.collections.queue.GigaBlockingQueue;
import org.openspaces.collections.serialization.DefaultSerializerProvider;
import org.openspaces.collections.serialization.ElementSerializer;
import org.openspaces.collections.serialization.ElementSerializerProvider;
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
    private Class elementType;
    private ElementSerializer serializer;
    private ElementSerializerProvider serializerProvider;
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

    public void setElementType(Class elementType) {
        this.elementType = elementType;
    }

    public void setSerializer(ElementSerializer serializer) {
        this.serializer = serializer;
    }

    public void setSerializerProvider(ElementSerializerProvider serializerProvider) {
        this.serializerProvider = serializerProvider;
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

        ElementSerializer serializer = pickSerializer();

        switch (collocationMode) {
            case EMBEDDED:
                if (capacity != null) {
                    gigaQueue = new EmbeddedGigaBlockingQueue(gigaSpace, queueName, capacity, serializer);
                } else {
                    gigaQueue = new EmbeddedGigaBlockingQueue(gigaSpace, queueName, serializer);
                }
                break;

            case LOCAL:
                // fall through
            case DISTRIBUTED:
                if (capacity != null) {
                    gigaQueue = new DistributedGigaBlockingQueue(gigaSpace, queueName, capacity, collocationMode, serializer);
                } else {
                    gigaQueue = new DistributedGigaBlockingQueue(gigaSpace, queueName, collocationMode, serializer);
                }
                break;
        }
    }

    public ElementSerializer pickSerializer() {
        if (serializer != null) {
            return serializer;
        }
        if (serializerProvider != null) {
            return serializerProvider.pickSerializer(elementType);
        }
        return new DefaultSerializerProvider().pickSerializer(elementType);
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
        gigaQueue.close();
    }
}