package org.dbsyncer.parser.model;

import com.alibaba.fastjson2.annotation.JSONField;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.model.ConnectorConfig;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/1 12:40
 */
public class Connector extends ConfigModel {

    public Connector() {
        super.setType(ConfigConstant.CONNECTOR);
    }

    /**
     * 连接器配置
     */
    private ConnectorConfig config;

    /**
     * 数据库列表
     */
    private List<String> databases;

    /**
     * 可作为源端，默认开启
     */
    @JSONField(name = "isSource")
    private boolean isSource = true;

    /**
     * 可作为目标端，默认开启
     */
    @JSONField(name = "isTarget")
    private boolean isTarget = true;

    public ConnectorConfig getConfig() {
        return config;
    }

    public Connector setConfig(ConnectorConfig config) {
        this.config = config;
        return this;
    }

    public List<String> getDatabases() {
        return databases;
    }

    public void setDatabases(List<String> databases) {
        this.databases = databases;
    }

    public boolean isSource() {
        return isSource;
    }

    public void setIsSource(boolean isSource) {
        this.isSource = isSource;
    }

    public boolean isTarget() {
        return isTarget;
    }

    public void setIsTarget(boolean isTarget) {
        this.isTarget = isTarget;
    }
}
