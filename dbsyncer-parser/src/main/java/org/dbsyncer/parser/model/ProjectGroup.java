package org.dbsyncer.parser.model;

import java.util.List;

/**
 * @author xinpeng.Fu
 * @version 1.0.0
 * @date 2022/6/9 17:09
 **/
public class ProjectGroup extends ConfigModel {

    /**
     * 连接器ID列表
     */
    private List<String> connectorIds;

    /**
     * 驱动ID列表
     */
    private List<String> mappingIds;

    public List<String> getConnectorIds() {
        return connectorIds;
    }

    public void setConnectorIds(List<String> connectorIds) {
        this.connectorIds = connectorIds;
    }

    public List<String> getMappingIds() {
        return mappingIds;
    }

    public void setMappingIds(List<String> mappingIds) {
        this.mappingIds = mappingIds;
    }
}