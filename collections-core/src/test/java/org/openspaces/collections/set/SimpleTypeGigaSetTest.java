package org.openspaces.collections.set;

import static org.openspaces.collections.CollectionUtils.MEDIUM_COLLECTION_SIZE;
import static org.openspaces.collections.CollectionUtils.createIntegerList;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.runners.Parameterized;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = "classpath:/single-space-test-config.xml")
public class SimpleTypeGigaSetTest extends AbstractGigaSetTest<Integer> {

    public SimpleTypeGigaSetTest(List<Integer> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return Arrays.asList(new Object[][] { { Collections.emptyList() },
                { Collections.singletonList(Integer.valueOf(1)) },
                { createIntegerList(MEDIUM_COLLECTION_SIZE) },
                /* { create(LARGE_COLLECTION_SIZE) } */
        });
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
