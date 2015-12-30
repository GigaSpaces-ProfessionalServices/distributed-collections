package org.openspaces.collections.util;

/**
 * @author Oleksiy_Dyagilev
 */
public class MiscUtils {

    public static boolean hasCause(Throwable e, Class<? extends Throwable> causeClass) {

        if (e.getCause() == null) {
            return false;
        } else if (e.getCause().getClass().equals(causeClass)) {
            return true;
        } else {
            return hasCause(e.getCause(), causeClass);
        }
    }

}
