package org.openspaces.core.collections;

import javax.annotation.Resource;

import org.junit.After;
import org.junit.Before;
import org.openspaces.core.GigaSpace;
import org.springframework.test.context.TestContextManager;

public abstract class GigaSpaceTest {

    @Resource
    protected GigaSpace gigaSpace;

    public void setUpSpringContext() {
        try {
            new TestContextManager(getClass()).prepareTestInstance(this);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set spring context up", e);
        }
    }
    
    @Before
    @After
    public void clear() {
        gigaSpace.clear(null);
    }
    
}
