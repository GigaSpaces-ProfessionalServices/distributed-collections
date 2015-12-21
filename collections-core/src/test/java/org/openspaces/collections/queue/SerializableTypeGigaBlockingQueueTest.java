package org.openspaces.collections.queue;

import static org.openspaces.collections.CollectionUtils.createComplexType;
import static org.openspaces.collections.CollectionUtils.createComplexTypeCollections;

import java.util.Collection;

import org.junit.Ignore;
import org.junit.runners.Parameterized;
import org.openspaces.collections.set.ComplexType;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = "classpath:/partitioned-space-test-config.xml")
@Ignore
public class SerializableTypeGigaBlockingQueueTest extends AbstractGigaBlockingQueueTest<ComplexType> {

    public SerializableTypeGigaBlockingQueueTest(Collection<ComplexType> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
        return createComplexTypeCollections();
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
