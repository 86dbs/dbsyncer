package org.dbsyncer.common.model;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.spi.ProxyApplicationContext;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/6/30 16:06
 */
public class IncrementConvertContext extends AbstractConvertContext {

    /**
     * 增量同步，事件（INSERT/UPDATE/DELETE）
     */
    private String event;

    /**
     * 增量同步，数据源数据
     */
    private Map source;

    /**
     * 增量同步，目标源数据
     */
    private Map target;

    public IncrementConvertContext(ProxyApplicationContext context, ConnectorMapper targetConnectorMapper, String targetTableName, String event, Map source, Map target) {
        this.context = context;
        this.targetConnectorMapper = targetConnectorMapper;
        this.targetTableName = targetTableName;
        this.event = event;
        this.source = source;
        this.target = target;
    }

    public String getEvent() {
        return event;
    }

    public Map getSource() {
        return source;
    }

    public Map getTarget() {
        return target;
    }
}