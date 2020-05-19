package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Mapping;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/03 17:20
 */
public class MappingVo extends Mapping {

    // 连接器
    private ConnectorVo sourceConnector;
    private ConnectorVo targetConnector;

    // 元信息
    private MetaVo meta;

    public MappingVo(ConnectorVo sourceConnector, ConnectorVo targetConnector, MetaVo meta) {
        this.sourceConnector = sourceConnector;
        this.targetConnector = targetConnector;
        this.meta = meta;
    }

    public ConnectorVo getSourceConnector() {
        return sourceConnector;
    }

    public ConnectorVo getTargetConnector() {
        return targetConnector;
    }

    public MetaVo getMeta() {
        return meta;
    }
}