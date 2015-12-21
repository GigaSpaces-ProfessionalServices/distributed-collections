package org.openspaces.collections.set;

import java.io.Serializable;
import java.util.Collection;

import javax.annotation.Resource;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openspaces.collections.AbstractCollectionTest;

@RunWith(Parameterized.class)
public abstract class AbstractGigaSetTest<T extends Serializable> extends AbstractCollectionTest<T> {

    @Resource
    protected GigaSet<T> gigaSet;
    
    public AbstractGigaSetTest(Collection<T> elements) {
        super(elements);
    }
    
    @Override
    protected Collection<T> getCollection() {
        return gigaSet;
    }
}
