package org.dbsyncer.sdk.connector;

import org.dbsyncer.sdk.model.ConnectorConfig;

/**
 * 连接器实例，管理连接生命周期
 *
 * @param <K> 配置
 * @param <V> 实例
 * @author AE86
 * @version 1.0.0
 * @date 2022/3/20 23:00
 */
public interface ConnectorInstance<K extends ConnectorConfig, V> extends Cloneable {

    /**
     * 获取服务地址
     *
     * @return
     */
    String getServiceUrl();

    /**
     * 获取连接配置
     *
     * @return
     */
    K getConfig();

    /**
     * 设置
     *
     * @param k
     */
    void setConfig(K k);

    /**
     * 获取连接通道实例
     *
     * @return
     * @throws Exception
     */
    V getConnection() throws Exception;

    /**
     * 关闭连接器
     */
    void close();

    /**
     * 浅拷贝连接器
     *
     * @return
     * @throws CloneNotSupportedException
     */
    Object clone() throws CloneNotSupportedException;
}