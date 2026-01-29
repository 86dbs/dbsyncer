package org.dbsyncer.biz.vo;

import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/03 17:20
 */
public class MappingVo extends Mapping {

    // 连接器
    private final Connector sourceConnector;
    private final Connector targetConnector;

    // 元信息
    private final MetaVo meta;

    public MappingVo(Connector sourceConnector, Connector targetConnector, MetaVo meta) {
        this.sourceConnector = sourceConnector;
        this.targetConnector = targetConnector;
        this.meta = meta;
    }

    public Connector getSourceConnector() {
        return sourceConnector;
    }

    public Connector getTargetConnector() {
        return targetConnector;
    }

    public MetaVo getMeta() {
        return meta;
    }
}