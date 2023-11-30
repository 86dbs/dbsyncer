/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector;

/**
 * 值转换器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2022/8/24 19:16
 */
public interface ValueMapper {

    /**
     * 转换值
     *
     * @param connectorInstance
     * @param val
     * @return
     */
    Object convertValue(ConnectorInstance connectorInstance, Object val) throws Exception;
}