package org.openspaces.collections;

import org.openspaces.collections.set.SerializableType;
import org.openspaces.collections.set.SerializableTypeBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public final class CollectionUtils {

    private CollectionUtils() {
    }

    public static final int MEDIUM_COLLECTION_SIZE = 100;

    public static final long LARGE_COLLECTION_SIZE = (long) Integer.MAX_VALUE + 1;

    public static List<SerializableType> createSerializableTypeList(long count) {
        List<SerializableType> data = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            data.add(createSerializableType());
        }
        return Collections.unmodifiableList(data);
    }

    public static SerializableType createSerializableType() {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        Long id = random.nextLong();
        return new SerializableTypeBuilder(id)
                .setNumber(random.nextLong())
                .setDescription("Test data" + id)
                .addChild(random.nextLong())
                .addChild(random.nextLong())
                .build();
    }
}
