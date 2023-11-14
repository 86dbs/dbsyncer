package org.dbsyncer.connector;

import org.dbsyncer.sdk.spi.ConnectorMapper;

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
     * @param connectorMapper
     * @param val
     * @return
     */
    Object convertValue(ConnectorMapper connectorMapper, Object val) throws Exception;
}