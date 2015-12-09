package org.openspaces.core.collections;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.runners.Parameterized;
import org.openspaces.core.collections.ComplexType.Child;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = "classpath:/partitioned-space-test-config.xml")
public class ComplexTypeGigaSetTest extends AbstractGigaSetTest<ComplexType> {

    private static final int MEDIUM_SET_SIZE = 500;
    private static final Set<ComplexType> MEDIUM_SET = create(MEDIUM_SET_SIZE);
    
    public ComplexTypeGigaSetTest(Set<ComplexType> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
       return Arrays.asList(new Object[][] {
          { Collections.emptySet() },
          { Collections.singleton(createComplexType())},
          { MEDIUM_SET }
       });
    }

    private static Set<ComplexType> create(long count) {
        Set<ComplexType> set = new HashSet<>();
        for (long i = 0; i < count; i++) {
            set.add(createComplexType());
        }
        return Collections.unmodifiableSet(set);
    }

    @Override
    protected ComplexType newNotNullElement() {
        return createComplexType();
    }
    
    private static ComplexType createComplexType() {
        ComplexType complexType = new ComplexType();
        Long id = ThreadLocalRandom.current().nextLong();
        complexType.setId(id);
        complexType.setNumber(ThreadLocalRandom.current().nextLong());
        complexType.setDescription("Test data" + id);
        
        Child child1 = new Child();
        child1.setId(ThreadLocalRandom.current().nextLong());
        child1.setParentId(id);
        
        Child child2 = new Child();
        child2.setId(ThreadLocalRandom.current().nextLong());
        child2.setParentId(id);
        
        complexType.setChildren(Arrays.asList(child1, child2));
        
        return complexType;
    }
}
