package org.openspaces.core.collections;

import java.util.ArrayList;
import java.util.List;

import org.openspaces.core.collections.ComplexType.Child;

public class ComplexTypeBuilder {

    private ComplexType complexType;
    
    public ComplexTypeBuilder(Long id) {
        complexType = new ComplexType();
        complexType.setId(id);
    }
    
    public ComplexTypeBuilder setNumber(Long number) {
        complexType.setNumber(number);
        return this;
    }

    public ComplexTypeBuilder setDescription(String description) {
        complexType.setDescription(description);
        return this;
    }

    public ComplexTypeBuilder addChild(Long childId) {
        List<Child> children = complexType.getChildren();
        if (children == null) {
            children = new ArrayList<>();
            complexType.setChildren(children);
        }
        Child child = new Child();
        child.setId(childId);
        child.setParentId(complexType.getId());
        children.add(child);
        return this;
    }
    
    public ComplexType build() {
        return complexType;
    }
}
