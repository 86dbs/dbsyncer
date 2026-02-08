/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.model.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * 驱动自定义表信息
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2026-01-04 21:41
 */
public class MappingCustomTableVO {

    /**
     * 驱动ID
     */
    private String id;

    /**
     * 驱动名称
     */
    private String name;

    /**
     * 元信息
     */
    private Meta meta;

    /**
     * 连接类型
     */
    private String connectorType;

    /**
     * 可扩展类型 {@link org.dbsyncer.sdk.enums.TableTypeEnum}
     */
    private String extendedType;

    /**
     * 主表列表
     */
    private List<Table> mainTables;

    /**
     * 自定义表列表
     */
    private List<TableVO> customTables = new ArrayList<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Meta getMeta() {
        return meta;
    }

    public void setMeta(Meta meta) {
        this.meta = meta;
    }

    public String getConnectorType() {
        return connectorType;
    }

    public void setConnectorType(String connectorType) {
        this.connectorType = connectorType;
    }

    public String getExtendedType() {
        return extendedType;
    }

    public void setExtendedType(String extendedType) {
        this.extendedType = extendedType;
    }

    public List<Table> getMainTables() {
        return mainTables;
    }

    public void setMainTables(List<Table> mainTables) {
        this.mainTables = mainTables;
    }

    public List<TableVO> getCustomTables() {
        return customTables;
    }

    public void setCustomTables(List<TableVO> customTables) {
        this.customTables = customTables;
    }
}
