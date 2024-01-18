package com.alibaba.datax.plugin.writer.orcfilewriter;

import org.apache.orc.TypeDescription;

public class ColumnType {
    private String type;
    private String name;

    private TypeDescription typeDescription;

    public ColumnType(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public String getType() {
        return this.type;
    }

    public TypeDescription toTypeDesc() {
        if (this.typeDescription == null) {
            this.typeDescription = TypeDescription.fromString(this.type);
        }

        return this.typeDescription;
    }

    @Override
    public String toString() {
        return "ColumnType{" +
                "type='" + type + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
