/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * CDC Agent 配置：单表存储 {@code dbsyncer_agent}。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-12
 */
public final class AgentStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.AGENT.getType();
    }
}
