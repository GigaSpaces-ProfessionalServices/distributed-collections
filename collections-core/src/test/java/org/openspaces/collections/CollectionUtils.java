package org.openspaces.collections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.openspaces.collections.set.ComplexType;
import org.openspaces.collections.set.ComplexTypeBuilder;

public final class CollectionUtils {

    private CollectionUtils() {
    }
    
    public static final int MEDIUM_COLLECTION_SIZE = 500;
    
    public static final long LARGE_COLLECTION_SIZE = (long)Integer.MAX_VALUE + 1;
    
    public static List<Integer> createIntegerList(long count) {
        List<Integer> data = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            data.add(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
        }
        return Collections.unmodifiableList(data);
    }
    
    public static List<ComplexType> createComplexTypeList(long count) {
        List<ComplexType> data = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            data.add(createComplexType());
        }
        return Collections.unmodifiableList(data);
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
