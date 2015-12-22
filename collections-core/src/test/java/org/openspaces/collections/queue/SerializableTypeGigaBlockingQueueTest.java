package org.openspaces.collections.queue;

import static org.openspaces.collections.CollectionUtils.MEDIUM_COLLECTION_SIZE;
import static org.openspaces.collections.CollectionUtils.createComplexType;
import static org.openspaces.collections.CollectionUtils.createComplexTypeList;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Ignore;
import org.junit.runners.Parameterized;
import org.openspaces.collections.set.ComplexType;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = "classpath:/partitioned-space-test-config.xml")
@Ignore
public class SerializableTypeGigaBlockingQueueTest extends AbstractGigaBlockingQueueTest<ComplexType> {

    public SerializableTypeGigaBlockingQueueTest(List<ComplexType> elements, int capacity) {
        super(elements, capacity);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return Arrays.asList(new Object[][] { 
                { Collections.emptyList(), 0 },
                { Collections.emptyList(), 1 },
                { Collections.singletonList(createComplexType()), 2 },
                { createComplexTypeList(MEDIUM_COLLECTION_SIZE), MEDIUM_COLLECTION_SIZE },
                /* { createComplexTypeList(LARGE_COLLECTION_SIZE) } */
        });
    }

    @Override
    protected Class<? extends ComplexType> getElementType() {
        return ComplexType.class;
    }

    @Override
    protected ComplexType[] getElementArray() {
        return new ComplexType[testedElements.size()];
    }

    @Override
    protected ComplexType newNotNullElement() {
        return createComplexType();
    }
}
