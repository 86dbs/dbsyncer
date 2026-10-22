/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.security;

/**
 * 登录失败计数与锁定截止时间（进程内状态）。
 *
 * @author wuji
 * @version 1.0.0
 */
public class LoginState {

    private final int failCount;

    /**
     * 锁定截止时间戳（毫秒）；未锁定为 0
     */
    private final long lockUntilMillis;

    public LoginState(int failCount, long lockUntilMillis) {
        this.failCount = failCount;
        this.lockUntilMillis = lockUntilMillis;
    }

    public int getFailCount() {
        return failCount;
    }

    public long getLockUntilMillis() {
        return lockUntilMillis;
    }
}
