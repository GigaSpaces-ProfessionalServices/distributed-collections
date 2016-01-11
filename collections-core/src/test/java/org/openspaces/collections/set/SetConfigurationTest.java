package org.openspaces.collections.set;

import org.openspaces.collections.CollectionUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import javax.annotation.Resource;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Leonid_Poliakov
 */
@ContextConfiguration
public class SetConfigurationTest extends AbstractTestNGSpringContextTests {
    @Resource(name = "myGigaSet")
    private GigaSet<SerializableType> set;

    @Test
    public void test() {
        assertNotNull(set);
        set.add(CollectionUtils.createSerializableType());
        assertEquals(set.size(), 1);
    }
}