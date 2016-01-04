package org.openspaces.collections.set;

import java.util.ArrayList;
import java.util.List;

public class NonSerializableTypeBuilder {

    private NonSerializableType complexType;

    public NonSerializableTypeBuilder(Long id) {
        complexType = new NonSerializableType();
        complexType.setId(id);
    }

    public NonSerializableTypeBuilder setNumber(Long number) {
        complexType.setNumber(number);
        return this;
    }

    public NonSerializableTypeBuilder setDescription(String description) {
        complexType.setDescription(description);
        return this;
    }

    public NonSerializableTypeBuilder addChild(Long childId) {
        List<NonSerializableType.Child> children = complexType.getChildren();
        if (children == null) {
            children = new ArrayList<>();
            complexType.setChildren(children);
        }
        NonSerializableType.Child child = new NonSerializableType.Child();
        child.setId(childId);
        child.setParentId(complexType.getId());
        children.add(child);
        return this;
    }

    public NonSerializableType build() {
        return complexType;
    }
}
