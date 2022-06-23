package org.dbsyncer.parser.model;

import org.dbsyncer.common.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author xinpeng.Fu
 * @version 1.0.0
 * @date 2022/6/9 17:09
 **/
public class ProjectGroup extends ConfigModel {

    /**
     * 链接列表
     */
    private List<String> connectorIds;


    /**
     * 映射id列表
     */
    private List<String> mappingIds;

    private List<Connector> connectors;

    private List<Mapping> mappings;

    public List<String> getConnectorIds() {
        return connectorIds;
    }

    public String getConnectorIdStr() {
        String connectorStr = CollectionUtils.isEmpty(connectorIds) ? "" : connectorIds.stream().collect(Collectors.joining(","));
        return connectorStr;
    }

    public void setConnectorIds(List<String> connectorIds) {
        this.connectorIds = connectorIds;
    }

    public List<String> getMappingIds() {
        return mappingIds;
    }

    public String getMappingIdStr() {
        String mappingStr = CollectionUtils.isEmpty(mappingIds) ? "" : mappingIds.stream().collect(Collectors.joining(","));
        return mappingStr;
    }
    public void setMappingIds(List<String> mappingIds) {
        this.mappingIds = mappingIds;
    }

    public List<Connector> getConnectors() {
        return connectors;
    }

    public void setConnectors(List<Connector> connectors) {
        this.connectors = connectors;
    }

    public List<Mapping> getMappings() {
        return mappings;
    }

    public void setMappings(List<Mapping> mappings) {
        this.mappings = mappings;
    }
}
