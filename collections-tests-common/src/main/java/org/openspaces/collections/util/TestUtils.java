package org.openspaces.collections.util;

import java.util.Arrays;
import java.util.Collection;

/**
 * @author Leonid_Poliakov
 */
public class TestUtils {
    public static Object[][] combination(Collection... collections) {
        if (collections.length == 0) {
            return new Object[][]{};
        }
        if (collections.length > 1) {
            return combine(collections[0], combination(Arrays.copyOfRange(collections, 1, collections.length)));
        }
        return singleParam(collections[0].toArray());
    }

    public static Object[][] singleParam(Object... values) {
        Object[][] params = new Object[values.length][];
        for (int index = 0; index < values.length; index++) {
            params[index] = new Object[]{values[index]};
        }
        return params;
    }

    private static Object[][] combine(Collection collection, Object[][] moreParams) {
        Object[][] params = new Object[collection.size() * moreParams.length][];
        int paramIndex = 0;
        for (Object value : collection) {
            for (Object[] paramArray : moreParams) {
                Object[] extendedArray = new Object[paramArray.length + 1];
                extendedArray[0] = value;
                System.arraycopy(paramArray, 0, extendedArray, 1, paramArray.length);

                params[paramIndex] = extendedArray;
                paramIndex++;
            }
        }
        return params;
    }
}