package org.dbsyncer.connector.config;

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

    public String getName() {
        return name;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getType() {
        return type;
    }

    public boolean isPk() {
        return pk;
    }

    public void setPk(boolean pk) {
        this.pk = pk;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("Field{").append("name='").append(name).append('\'')
                .append(", typeName='").append(typeName).append('\'')
                .append(", type=").append(type)
                .append(", pk=").append(pk).append('}').toString();
    }
}