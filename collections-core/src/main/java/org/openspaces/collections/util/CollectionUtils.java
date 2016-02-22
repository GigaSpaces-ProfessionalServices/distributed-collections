package org.openspaces.collections.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class CollectionUtils {
	
	private static final int ITEM_NOT_FOUND = -1;
	
	public static int containsUsingDeepCompare(Object o, List<Object> items) {
		
		int collectionSize = items.size();
		
		for (int index = 0; index < collectionSize; index++){
            	if (Objects.deepEquals(o, items.get(index))) {
                    return index;
                }
        }
        
        return ITEM_NOT_FOUND;
    }

	//TODO:Apache Commons Clone
	public static List<Object> cloneCollection(List<Object> originalQueue){
		if(originalQueue instanceof ArrayList){
			return (List<Object>)((ArrayList<Object>)originalQueue).clone();
		}else{
			throw new IllegalArgumentException("Collection must be of type ArrayList");
		}
	}
	public static Set<Object> cloneSet(Set<Object> originalSet){
		if(originalSet instanceof HashSet){
			return (HashSet<Object>)((HashSet<Object>)originalSet).clone();
		}else{
			throw new IllegalArgumentException("Set must be of type HashSet");
		}
	}
}
