package org.openspaces.collections.queue;

import static org.openspaces.collections.CollectionUtils.MEDIUM_COLLECTION_SIZE;
import static org.openspaces.collections.CollectionUtils.createIntegerList;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Ignore;
import org.junit.runners.Parameterized;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = "classpath:/single-space-test-config.xml")
@Ignore
public class SimpleTypeGigaBlockingQueueTest extends AbstractGigaBlockingQueueTest<Integer> {

    public SimpleTypeGigaBlockingQueueTest(List<Integer> elements, int capacity) {
        super(elements, capacity);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
            return Arrays.asList(new Object[][] { 
                    { Collections.emptyList(), 0},
                    { Collections.emptyList(), 1},
                    { Collections.singletonList(Integer.valueOf(1)), 2},
                    { createIntegerList(MEDIUM_COLLECTION_SIZE), MEDIUM_COLLECTION_SIZE},
                    /* { create(LARGE_COLLECTION_SIZE) } */
            });
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
        return ThreadLocalRandom.current().nextInt();
    }
}