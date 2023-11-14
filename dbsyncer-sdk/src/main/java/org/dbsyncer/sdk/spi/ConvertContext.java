package org.dbsyncer.sdk.spi;

import java.util.List;
import java.util.Map;

/**
 * 插件转换上下文
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/10/28 20:26
 */
public interface ConvertContext {

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
     * Spring上下文
     */
    ProxyApplicationContext getContext();

    /**
     * 数据源连接实例
     */
    ConnectorMapper getSourceConnectorMapper();

    /**
     * 目标源连接实例
     */
    ConnectorMapper getTargetConnectorMapper();

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

}