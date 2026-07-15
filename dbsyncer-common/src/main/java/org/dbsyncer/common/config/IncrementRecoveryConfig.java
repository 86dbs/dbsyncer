/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 增量同步（含全量+增量）自动恢复重试配置。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-14 15:30
 */
@Configuration
@ConfigurationProperties(prefix = "dbsyncer.manager.increment.recovery")
public class IncrementRecoveryConfig {

    /**
     * 自动恢复启动 CDC 监听失败时的最大重试次数
     */
    private int retryTimes = 10;

    /**
     * 每次重试间隔（毫秒）
     */
    private long retryInterval = 5000L;

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public long getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
    }
}
