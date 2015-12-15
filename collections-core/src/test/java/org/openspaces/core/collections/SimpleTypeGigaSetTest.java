package org.openspaces.core.collections;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.runners.Parameterized;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = "classpath:/single-space-test-config.xml")
public class SimpleTypeGigaSetTest extends AbstractGigaSetTest<Integer> {

    private static final int MEDIUM_SET_SIZE = 500;
    private static final Set<Integer> MEDIUM_SET = create(MEDIUM_SET_SIZE);
    
    private static final long LARGE_SET_SIZE = (long)Integer.MAX_VALUE + 1;
   // private static final Set<Integer> LARGE_SET = create(LARGE_SET_SIZE);
    
    public SimpleTypeGigaSetTest(Set<Integer> elements) {
        super(elements);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testData() {
       return Arrays.asList(new Object[][] {
          { Collections.emptySet() },
          { Collections.singleton(Integer.valueOf(1))},
          { MEDIUM_SET },
          /*{ LARGE_SET }*/
       });
    }

    private static Set<Integer> create(long count) {
        Set<Integer> set = new HashSet<>();
        for (long i = 0; i < count; i++) {
            set.add(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
        }
        return Collections.unmodifiableSet(set);
    }

    @Override
    protected Integer newNotNullElement() {
        return ThreadLocalRandom.current().nextInt(Integer.MIN_VALUE, 0);
    }

    @Override
    protected Class<? extends Integer> getElementType() {
        return Integer.class;
    }
}
