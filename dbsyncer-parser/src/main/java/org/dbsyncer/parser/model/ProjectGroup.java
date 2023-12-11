package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.constant.ConfigConstant;

import java.util.List;

/**
 * @author xinpeng.Fu
 * @version 1.0.0
 * @date 2022/6/9 17:09
 **/
public class ProjectGroup extends ConfigModel {

    public ProjectGroup() {
        super.setType(ConfigConstant.PROJECT_GROUP);
    }

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