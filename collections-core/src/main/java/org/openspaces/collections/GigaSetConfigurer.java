package org.openspaces.collections;

import org.openspaces.collections.set.GigaSet;
import org.openspaces.core.GigaSpace;

import java.io.Serializable;

/**
 * @author Leonid_Poliakov
 */
public class GigaSetConfigurer<T extends Serializable> {
    private final GigaSetFactoryBean<T> gigaSetFactoryBean = new GigaSetFactoryBean<>();
    private GigaSet<T> gigaSet;

    public GigaSetConfigurer(GigaSpace gigaSpace) {
        gigaSetFactoryBean.setGigaSpace(gigaSpace);
    }

    public GigaSetConfigurer<T> clustered(Boolean clustered) {
        gigaSetFactoryBean.setClustered(clustered);
        return this;
    }

    public GigaSet<T> gigaSet() {
        if (gigaSet == null) {
            gigaSetFactoryBean.afterPropertiesSet();
            gigaSet = gigaSetFactoryBean.getObject();
        }
        return this.gigaSet;
    }
}