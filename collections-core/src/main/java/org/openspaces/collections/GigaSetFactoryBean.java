package org.openspaces.collections;

import com.j_spaces.map.IMap;
import org.openspaces.collections.serialization.DefaultSerializerProvider;
import org.openspaces.collections.serialization.ElementSerializer;
import org.openspaces.collections.set.DefaultGigaSet;
import org.openspaces.collections.set.GigaSet;
import org.openspaces.core.GigaMap;
import org.openspaces.core.GigaMapConfigurer;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.map.MapConfigurer;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.io.Serializable;

/**
 * TODO: add gigamap and map features, javadocs
 *
 * @author Leonid_Poliakov
 */
public class GigaSetFactoryBean<T extends Serializable> implements InitializingBean, DisposableBean, FactoryBean, BeanNameAware {
    private String beanName;
    private GigaSpace gigaSpace;
    private Boolean clustered;
    private DefaultGigaSet<T> gigaSet;

    public void setGigaSpace(GigaSpace gigaSpace) {
        this.gigaSpace = gigaSpace;
    }

    public void setClustered(Boolean clustered) {
        this.clustered = clustered;
    }

    @Override
    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(gigaSpace, "gigaSpace property must be set");
        MapConfigurer mapConfigurer = new MapConfigurer(gigaSpace.getSpace());
        if (clustered != null) {
            mapConfigurer.clustered(clustered);
        }
        IMap map = mapConfigurer.createMap();
        GigaMap gigaMap = new GigaMapConfigurer(map).gigaMap();
        gigaSet = new DefaultGigaSet<>(gigaMap);
    }

    @Override
    public GigaSet<T> getObject() {
        return gigaSet;
    }

    @Override
    public Class<? extends GigaSet> getObjectType() {
        return gigaSet == null ? GigaSet.class : gigaSet.getClass();
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void destroy() throws Exception {
    }
}