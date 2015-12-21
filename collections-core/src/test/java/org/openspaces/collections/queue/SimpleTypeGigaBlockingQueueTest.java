package org.openspaces.collections.queue;

import static org.openspaces.collections.CollectionUtils.createIntegerCollections;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Ignore;
import org.junit.runners.Parameterized;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = "classpath:/single-space-test-config.xml")
@Ignore
public class SimpleTypeGigaBlockingQueueTest extends AbstractGigaBlockingQueueTest<Integer> {

    public SimpleTypeGigaBlockingQueueTest(Collection<Integer> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return createIntegerCollections();
    }

    @Override
    protected Class<? extends Integer> getElementType() {
        return Integer.class;
    }

    @Override
    protected Integer[] getElementArray() {
        return new Integer[testedElements.size()];
    }

    @Override
    protected Integer newNotNullElement() {
        return ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, 0);
    }
}