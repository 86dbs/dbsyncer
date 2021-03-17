package org.dbsyncer.connector.config;

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

    public Field() {
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

    @Override
    public String toString() {
        return JsonUtil.objToJson(this);
    }
}