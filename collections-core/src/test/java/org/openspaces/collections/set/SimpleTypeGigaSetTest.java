package org.openspaces.collections.set;

import static org.openspaces.collections.CollectionUtils.createIntegerCollections;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.runners.Parameterized;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = "classpath:/single-space-test-config.xml")
public class SimpleTypeGigaSetTest extends AbstractGigaSetTest<Integer> {

    public SimpleTypeGigaSetTest(Set<Integer> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return createIntegerCollections();
    }

    @Override
    protected Integer newNotNullElement() {
        return ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, 0);
    }

    @Override
    protected Class<? extends Integer> getElementType() {
        return Integer.class;
    }

    @Override
    protected Integer[] getElementArray() {
        return new Integer[testedElements.size()];
    }
}
