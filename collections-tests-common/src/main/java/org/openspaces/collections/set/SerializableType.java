package org.openspaces.collections.set;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class SerializableType implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long id;
    private Long number;
    private String description;
    private List<Child> children;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getNumber() {
        return number;
    }

    public void setNumber(Long number) {
        this.number = number;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<Child> getChildren() {
        return children;
    }

    public void setChildren(List<Child> children) {
        this.children = children;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((children == null) ? 0 : children.hashCode());
        result = prime * result + ((description == null) ? 0 : description.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((number == null) ? 0 : number.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SerializableType other = (SerializableType) obj;
        if (!Objects.equals(id, other.id)) {
            return false;
        }
        if (!Objects.equals(number, other.number)) {
            return false;
        }
        return Objects.equals(description, other.description) ? Objects.equals(children, other.children) : false;
    }

    @Override
    public String toString() {
        return "SerializableType [id=" + id + ", number=" + number + ", description=" + description
                + ", children=" + children + "]";
    }

    public static class Child implements Serializable {
        private Long id;
        private Long parentId;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Long getParentId() {
            return parentId;
        }

        public void setParentId(Long parentId) {
            this.parentId = parentId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((parentId == null) ? 0 : parentId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Child other = (Child) obj;
            return Objects.equals(id, other.id) ? Objects.equals(parentId, other.parentId) : false;
        }

        @Override
        public String toString() {
            return "Child [id=" + id + ", parentId=" + parentId + "]";
        }
    }
}
