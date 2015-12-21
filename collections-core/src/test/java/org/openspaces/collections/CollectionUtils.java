package org.openspaces.collections;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.openspaces.collections.set.ComplexType;
import org.openspaces.collections.set.ComplexTypeBuilder;

public final class CollectionUtils {

    private CollectionUtils() {
    }
    
    private static final int MEDIUM_SET_SIZE = 500;
    
    private static final long LARGE_SET_SIZE = (long)Integer.MAX_VALUE + 1;
    
    public static Collection<Object[]> createIntegerCollections() {
       return Arrays.asList(new Object[][] {
          { Collections.emptySet() },
          { Collections.singleton(Integer.valueOf(1))},
          { createIntegerSet(MEDIUM_SET_SIZE) },
          /*{ create(LARGE_SET) }*/
       });
    }

    private static Set<Integer> createIntegerSet(long count) {
        Set<Integer> set = new HashSet<>();
        for (long i = 0; i < count; i++) {
            set.add(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
        }
        return Collections.unmodifiableSet(set);
    }
    
    public static Collection<Object[]> createComplexTypeCollections() {
        return Arrays.asList(new Object[][] {
           { Collections.emptySet() },
           { Collections.singleton(createComplexType())},
           { createComplexTypeSet(MEDIUM_SET_SIZE) }
        });
     }
    
    private static Set<ComplexType> createComplexTypeSet(long count) {
        Set<ComplexType> set = new HashSet<>();
        for (long i = 0; i < count; i++) {
            set.add(createComplexType());
        }
        return Collections.unmodifiableSet(set);
    }
    
    public static ComplexType createComplexType() {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        Long id = random.nextLong();
        return new ComplexTypeBuilder(id)
            .setNumber(random.nextLong())
            .setDescription("Test data" + id)
            .addChild(random.nextLong())
            .addChild(random.nextLong())
            .build();
    }
}
