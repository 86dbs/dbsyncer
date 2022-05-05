package org.dbsyncer.connector.model;

import org.dbsyncer.common.util.JsonUtil;

/**
 * 字段属性
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/30 15:10
 */
public class Field {

    /**
     * 字段名，ID
     */
    private String name;

    /**
     * 类型名，INT
     */
    private String typeName;

    /**
     * 类型编码，4
     */
    private int type;

    /**
     * 主键
     */
    private boolean pk;

    /**
     * 字段别名
     */
    private String labelName;

    /**
     * 是否系统字段
     */
    private boolean unmodifiabled;

    public Field() {
    }

    public Field(String name, String labelName) {
        this.name = name;
        this.labelName = labelName;
    }

    public Field(String name, String typeName, int type) {
        this.name = name;
        this.typeName = typeName;
        this.type = type;
    }

    public Field(String name, String typeName, int type, boolean pk) {
        this.name = name;
        this.typeName = typeName;
        this.type = type;
        this.pk = pk;
    }

    public Field(String name, String typeName, int type, boolean pk, String labelName, boolean unmodifiabled) {
        this.name = name;
        this.typeName = typeName;
        this.type = type;
        this.pk = pk;
        this.labelName = labelName;
        this.unmodifiabled = unmodifiabled;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public boolean isPk() {
        return pk;
    }

    public void setPk(boolean pk) {
        this.pk = pk;
    }

    public String getLabelName() {
        return labelName;
    }

    public Field setLabelName(String labelName) {
        this.labelName = labelName;
        return this;
    }

    public boolean isUnmodifiabled() {
        return unmodifiabled;
    }

    public Field setUnmodifiabled(boolean unmodifiabled) {
        this.unmodifiabled = unmodifiabled;
        return this;
    }

    @Override
    public String toString() {
        return JsonUtil.objToJson(this);
    }
}