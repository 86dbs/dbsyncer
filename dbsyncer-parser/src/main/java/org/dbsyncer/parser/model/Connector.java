package org.dbsyncer.parser.model;

import org.dbsyncer.sdk.model.ConnectorConfig;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.constant.ConfigConstant;

import java.util.ArrayList;
import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 12:40
 */
public class Connector extends ConfigModel{

    public Connector() {
        super.setType(ConfigConstant.CONNECTOR);
    }

    /**
     * 表名,["MY_USER", "T_MY_USER", "table_999"]
     */
    private List<Table> table = new ArrayList<>();

    /**
     * 连接器配置
     */
    private ConnectorConfig config;

    public List<Table> getTable() {
        return table;
    }

    public Connector setTable(List<Table> table) {
        this.table = table;
        return this;
    }

    public ConnectorConfig getConfig() {
        return config;
    }

    public Connector setConfig(ConnectorConfig config) {
        this.config = config;
        return this;
    }
}
