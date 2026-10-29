/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

/**
 * 同步任务列表行（仅列表展示字段，不含完整配置）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-29
 */
public class MappingListVO {

    private String id;
    private String name;
    private String model;
    private MappingListConnectorVO sourceConnector;
    private MappingListConnectorVO targetConnector;
    private MappingListMetaVO meta;

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

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public MappingListConnectorVO getSourceConnector() {
        return sourceConnector;
    }

    public void setSourceConnector(MappingListConnectorVO sourceConnector) {
        this.sourceConnector = sourceConnector;
    }

    public MappingListConnectorVO getTargetConnector() {
        return targetConnector;
    }

    public void setTargetConnector(MappingListConnectorVO targetConnector) {
        this.targetConnector = targetConnector;
    }

    public MappingListMetaVO getMeta() {
        return meta;
    }

    public void setMeta(MappingListMetaVO meta) {
        this.meta = meta;
    }
}
