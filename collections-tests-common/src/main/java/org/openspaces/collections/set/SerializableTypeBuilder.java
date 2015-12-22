package org.openspaces.collections.set;

import java.util.ArrayList;
import java.util.List;

public class SerializableTypeBuilder {

    private SerializableType complexType;
    
    public SerializableTypeBuilder(Long id) {
        complexType = new SerializableType();
        complexType.setId(id);
    }
    
    public SerializableTypeBuilder setNumber(Long number) {
        complexType.setNumber(number);
        return this;
    }

    public SerializableTypeBuilder setDescription(String description) {
        complexType.setDescription(description);
        return this;
    }

    public SerializableTypeBuilder addChild(Long childId) {
        List<SerializableType.Child> children = complexType.getChildren();
        if (children == null) {
            children = new ArrayList<>();
            complexType.setChildren(children);
        }
        SerializableType.Child child = new SerializableType.Child();
        child.setId(childId);
        child.setParentId(complexType.getId());
        children.add(child);
        return this;
    }
    
    public SerializableType build() {
        return complexType;
    }
}
