package org.dbsyncer.sdk.model;

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
     * 字段大小
     */
    private long columnSize;

    /**
     * 字段比例
     */
    private int ratio;

    /**
     * 空间参考系统标识符（SRID），用于Geometry类型
     * 例如：4326 (WGS84), 3857 (Web Mercator) 等
     */
    private Integer srid;

    /**
     * 字段长度是否固定
     * true: 固定长度（如CHAR、NCHAR、BINARY）
     * false: 可变长度（如VARCHAR、NVARCHAR、VARBINARY）
     * null: 未设置或不适用（如数值类型、日期类型等）
     */
    private Boolean isSizeFixed;

    /**
     * 字段是否允许为空
     * true: 允许为空（NULL）
     * false: 不允许为空（NOT NULL）
     * null: 未设置（使用数据库默认值）
     */
    private Boolean nullable;

    /**
     * 字段注释
     * 例如：'年龄', '用户ID' 等
     */
    private String comment;

    /**
     * 字段是否自增
     * true: 自增字段（如 MySQL 的 AUTO_INCREMENT，SQL Server 的 IDENTITY）
     * false: 非自增字段
     */
    private boolean autoincrement;

    public Field() {
    }

    public Field(String name, String typeName, int type) {
        this.name = name;
        this.typeName = typeName.toUpperCase();
        this.type = type;
    }

    public Field(String name, String typeName, int type, boolean pk) {
        this.name = name;
        this.typeName = typeName.toUpperCase();
        this.type = type;
        this.pk = pk;
    }

    public Field(String name, String typeName, int type, boolean pk, long columnSize, int ratio) {
        this.name = name;
        this.typeName = typeName.toUpperCase();
        this.type = type;
        this.pk = pk;
        this.columnSize = columnSize;
        this.ratio = ratio;
    }

    public Field(String name, String typeName, int type, boolean pk, long columnSize, int ratio, Integer srid) {
        this.name = name;
        this.typeName = typeName.toUpperCase();
        this.type = type;
        this.pk = pk;
        this.columnSize = columnSize;
        this.ratio = ratio;
        this.srid = srid;
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
        this.typeName = typeName.toUpperCase();
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

    public void setLabelName(String labelName) {
        this.labelName = labelName;
    }

    public long getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(long columnSize) {
        this.columnSize = columnSize;
    }

    public int getRatio() {
        return ratio;
    }

    public void setRatio(int ratio) {
        this.ratio = ratio;
    }

    /**
     * 获取空间参考系统标识符（SRID）
     * @return SRID值，如果未设置则返回null
     */
    public Integer getSrid() {
        return srid;
    }

    /**
     * 设置空间参考系统标识符（SRID）
     * @param srid SRID值，例如：4326 (WGS84), 3857 (Web Mercator) 等
     */
    public void setSrid(Integer srid) {
        this.srid = srid;
    }

    /**
     * 获取字段长度是否固定
     * @return true表示固定长度，false表示可变长度，null表示未设置或不适用
     */
    public Boolean getIsSizeFixed() {
        return isSizeFixed;
    }

    /**
     * 设置字段长度是否固定
     * @param isSizeFixed true表示固定长度（如CHAR、NCHAR、BINARY），false表示可变长度（如VARCHAR、NVARCHAR、VARBINARY），null表示未设置或不适用
     */
    public void setIsSizeFixed(Boolean isSizeFixed) {
        this.isSizeFixed = isSizeFixed;
    }

    /**
     * 获取字段是否允许为空
     * @return true表示允许为空，false表示不允许为空，null表示未设置
     */
    public Boolean getNullable() {
        return nullable;
    }

    /**
     * 设置字段是否允许为空
     * @param nullable true表示允许为空（NULL），false表示不允许为空（NOT NULL），null表示未设置
     */
    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    /**
     * 获取字段注释
     * @return 注释字符串，如果未设置则返回null
     */
    public String getComment() {
        return comment;
    }

    /**
     * 设置字段注释
     * @param comment 注释字符串，例如：'年龄', '用户ID' 等
     */
    public void setComment(String comment) {
        this.comment = comment;
    }

    /**
     * 获取字段是否自增
     * @return true表示自增字段，false表示非自增字段
     */
    public boolean isAutoincrement() {
        return autoincrement;
    }

    /**
     * 设置字段是否自增
     * @param autoincrement true表示自增字段（如 MySQL 的 AUTO_INCREMENT，SQL Server 的 IDENTITY），false表示非自增字段
     */
    public void setAutoincrement(boolean autoincrement) {
        this.autoincrement = autoincrement;
    }

    @Override
    public String toString() {
        return JsonUtil.objToJson(this);
    }
}