/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.security;

/**
 * 一次登录失败后的计数与锁定状态，用于拼装提示文案。
 *
 * @author wuji
 * @version 1.0.0
 */
public final class LoginResult {

    private final int failCount;
    private final int remainingAttempts;
    private final boolean locked;
    private final int lockMinutes;
    private final long remainingLockMinutes;

    public LoginResult(int failCount, int remainingAttempts, boolean locked,
                       int lockMinutes, long remainingLockMinutes) {
        this.failCount = failCount;
        this.remainingAttempts = remainingAttempts;
        this.locked = locked;
        this.lockMinutes = lockMinutes;
        this.remainingLockMinutes = remainingLockMinutes;
    }

    public int getFailCount() {
        return failCount;
    }

    public int getRemainingAttempts() {
        return remainingAttempts;
    }

    public boolean isLocked() {
        return locked;
    }

    public int getLockMinutes() {
        return lockMinutes;
    }

    public long getRemainingLockMinutes() {
        return remainingLockMinutes;
    }

    /**
     * 密码错误且尚未锁定。
     */
    public String badCredentialsMessage() {
        return String.format("账号或密码错误，已失败%d次，还剩%d次机会；连续失败%d次将锁定%d分钟",
                failCount, remainingAttempts, LoginService.MAX_ATTEMPTS, lockMinutes);
    }

    /**
     * 本次失败后刚触发锁定，或已在锁定期内。
     */
    public String lockedMessage() {
        long minutes = Math.max(1L, remainingLockMinutes);
        return String.format("账号已锁定，约剩余%d分钟，请稍后再试或联系管理员解锁", minutes);
    }
}
