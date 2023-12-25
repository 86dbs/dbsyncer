/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.connector;

import java.util.Map;

/**
 * 连接器配置校验器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-07 23:17
 */
public interface ConfigValidator<C> {

    /**
     * 修改配置
     *
     * @param connectorConfig
     * @param params
     * @return
     */
    void modify(C connectorConfig, Map<String, String> params);

}