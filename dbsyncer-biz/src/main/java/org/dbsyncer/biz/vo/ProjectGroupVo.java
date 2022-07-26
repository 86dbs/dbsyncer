package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.ProjectGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/7/18 0:25
 */
public class ProjectGroupVo extends ProjectGroup {

    private int connectorSize;

    private List<Connector> connectors = new ArrayList<>();

    private List<MappingVo> mappings = new ArrayList<>();

    public int getConnectorSize() {
        return connectorSize;
    }

    public void setConnectorSize(int connectorSize) {
        this.connectorSize = connectorSize;
    }

    public List<Connector> getConnectors() {
        return connectors;
    }

    public void setConnectors(List<Connector> connectors) {
        this.connectors = connectors;
    }

    public List<MappingVo> getMappings() {
        return mappings;
    }

    public void setMappings(List<MappingVo> mappings) {
        this.mappings = mappings;
    }
}