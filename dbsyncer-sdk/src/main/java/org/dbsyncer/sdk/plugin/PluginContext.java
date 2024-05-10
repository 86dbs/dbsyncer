package org.dbsyncer.sdk.plugin;

import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.enums.ModelEnum;

import java.util.List;
import java.util.Map;

/**
 * 插件转换上下文
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/10/28 20:26
 */
public interface PluginContext {

    /**
     * 获取同步方式
     *
     * @return
     */
    ModelEnum getModelEnum();

    /**
     * 是否终止同步数据到目标源库
     *
     * @return
     */
    boolean isTerminated();

    /**
     * 是否终止同步数据到目标源库
     * <p>true: 终止，默认值false
     *
     * @param terminated
     */
    void setTerminated(boolean terminated);

    /**
     * 数据源连接实例
     */
    ConnectorInstance getSourceConnectorInstance();

    /**
     * 目标源连接实例
     */
    ConnectorInstance getTargetConnectorInstance();

    /**
     * 数据源表
     */
    String getSourceTableName();

    /**
     * 目标源表
     */
    String getTargetTableName();

    /**
     * 增量同步，事件（INSERT/UPDATE/DELETE）
     */
    String getEvent();

    /**
     * 数据源数据集合
     */
    List<Map> getSourceList();

    /**
     * 目标源源数据集合
     */
    List<Map> getTargetList();

    /**
     * 获取插件参数
     *
     * @return
     */
    String getPluginExtInfo();

}