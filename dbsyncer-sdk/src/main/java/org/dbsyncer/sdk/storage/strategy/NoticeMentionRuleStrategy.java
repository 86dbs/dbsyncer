/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.storage.strategy;

import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.Strategy;

/**
 * 告警 @ 覆盖规则：单表存储
 *
 * @author wuji
 * @version 1.0.0
 */
public final class NoticeMentionRuleStrategy implements Strategy {

    @Override
    public String createSharding(String separator, String collectionId) {
        return StorageEnum.NOTICE_MENTION_RULE.getType();
    }
}
