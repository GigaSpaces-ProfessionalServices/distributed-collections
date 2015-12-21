package org.openspaces.collections.set;

import static org.openspaces.collections.CollectionUtils.createComplexType;
import static org.openspaces.collections.CollectionUtils.createComplexTypeCollections;

import java.util.Collection;
import java.util.Set;

import org.junit.runners.Parameterized;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = "classpath:/partitioned-space-test-config.xml")
public class ComplexTypeGigaSetTest extends AbstractGigaSetTest<ComplexType> {

    public ComplexTypeGigaSetTest(Set<ComplexType> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
       return createComplexTypeCollections();
    }

    @Override
    protected ComplexType newNotNullElement() {
        return createComplexType();
    }
    
    @Override
    protected Class<? extends ComplexType> getElementType() {
        return ComplexType.class;
    }

    @Override
    protected ComplexType[] getElementArray() {
        return new ComplexType[testedElements.size()];
    }
}
