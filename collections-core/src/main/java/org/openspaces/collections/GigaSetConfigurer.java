package org.openspaces.collections;

import org.openspaces.collections.set.GigaSet;
import org.openspaces.core.GigaSpace;

/**
 * @author Leonid_Poliakov
 */
public class GigaSetConfigurer {
    private final GigaSetFactoryBean gigaSetFactoryBean = new GigaSetFactoryBean();
    private GigaSet gigaSet;

    public GigaSetConfigurer(GigaSpace gigaSpace) {
        gigaSetFactoryBean.setGigaSpace(gigaSpace);
    }

    public GigaSetConfigurer clustered(Boolean clustered) {
        gigaSetFactoryBean.setClustered(clustered);
        return this;
    }

    public GigaSet gigaSet() {
        if (gigaSet == null) {
            gigaSetFactoryBean.afterPropertiesSet();
            gigaSet = (GigaSet) gigaSetFactoryBean.getObject();
        }

        return this.gigaSet;
    }
}