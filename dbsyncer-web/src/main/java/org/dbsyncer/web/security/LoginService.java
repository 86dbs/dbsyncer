/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.security;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.UserInfo;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 登录失败次数与账号锁定。
 * 已存在用户落库 {@code dbsyncer_user.LOGIN_FAIL_COUNT / LOCK_UNTIL}，集群节点共享；
 * 不存在的用户名仍用进程内缓存限流（防探测），重启清空。
 *
 * @author wuji
 * @version 1.0.0
 */
@Service
public class LoginService {

    /**
     * 连续失败上限
     */
    public static final int MAX_ATTEMPTS = 3;

    @Resource
    private UserProfile userProfile;

    @Resource
    private SystemConfigProfile systemConfigProfile;

    /**
     * 未知用户名的进程内失败计数（不落库）
     */
    private final ConcurrentHashMap<String, LoginState> unknownAttempts = new ConcurrentHashMap<>();

    /**
     * 当前配置的锁定时长（分钟），非法或未配置时回退系统配置默认值。
     */
    public int getLockMinutes() {
        SystemConfig config = systemConfigProfile.getSystemConfig();
        int minutes = config == null ? 0 : config.getLoginLockMinutes();
        return minutes >= 1 && minutes <= 1440 ? minutes : new SystemConfig().getLoginLockMinutes();
    }

    /**
     * 账号是否处于锁定中（过期则清零并返回 false）。
     */
    public boolean isLocked(String username) {
        return resolveLockUntil(username) > System.currentTimeMillis();
    }

    /**
     * 剩余锁定分钟数（向上取整，至少 1；未锁定返回 0）。
     */
    public long getRemainingLockMinutes(String username) {
        long lockUntil = resolveLockUntil(username);
        long now = System.currentTimeMillis();
        if (lockUntil <= now) {
            return 0L;
        }
        long minutes = TimeUnit.MILLISECONDS.toMinutes(lockUntil - now);
        long rem = (lockUntil - now) % TimeUnit.MINUTES.toMillis(1);
        return rem > 0 ? minutes + 1 : Math.max(1L, minutes);
    }

    /**
     * 记录一次登录失败；达到阈值则锁定。
     */
    public LoginResult onFailure(String username) {
        int lockMinutes = getLockMinutes();
        if (StringUtil.isBlank(username)) {
            return new LoginResult(MAX_ATTEMPTS, false, lockMinutes, 0L);
        }
        Boolean locked = userProfile.recordLoginFailure(username, MAX_ATTEMPTS, getLockMillis(lockMinutes));
        if (locked == null) {
            return onUnknownFailure(username, lockMinutes);
        }
        UserInfo user = userProfile.getByUsername(username);
        int failCount = user == null ? 0 : user.getLoginFailCount();
        boolean isLocked = Boolean.TRUE.equals(locked);
        int remaining = Math.max(0, MAX_ATTEMPTS - failCount);
        long remainingLockMinutes = isLocked ? getRemainingLockMinutes(username) : 0L;
        return new LoginResult(remaining, isLocked, lockMinutes, remainingLockMinutes);
    }

    /**
     * 当前锁定提示（已在锁定期内访问登录）。
     */
    public LoginResult lockedResult(String username) {
        long remaining = getRemainingLockMinutes(username);
        return new LoginResult(0, true, getLockMinutes(), remaining);
    }

    /**
     * 登录成功，清除失败计数与锁定。
     */
    public void onSuccess(String username) {
        unlock(username);
    }

    /**
     * 管理员解锁或登录成功清零。
     */
    public void unlock(String username) {
        if (StringUtil.isBlank(username)) {
            return;
        }
        if (!userProfile.updateLoginLock(username, 0, 0L)) {
            unknownAttempts.remove(username);
        }
    }

    private long getLockMillis(int lockMinutes) {
        return lockMinutes * 60 * 1000L;
    }

    private long resolveLockUntil(String username) {
        if (StringUtil.isBlank(username)) {
            return 0L;
        }
        UserInfo user = userProfile.getByUsername(username);
        if (user == null) {
            LoginState state = unknownAttempts.get(username);
            if (state == null) {
                return 0L;
            }
            long lockUntil = state.getLockUntilMillis();
            if (lockUntil <= 0) {
                return 0L;
            }
            long now = System.currentTimeMillis();
            if (now < lockUntil) {
                return lockUntil;
            }
            unknownAttempts.remove(username, state);
            return 0L;
        }
        long lockUntil = user.getLockUntil();
        if (lockUntil <= 0) {
            return 0L;
        }
        long now = System.currentTimeMillis();
        if (now < lockUntil) {
            return lockUntil;
        }
        userProfile.updateLoginLock(username, 0, 0L);
        return 0L;
    }

    private LoginResult onUnknownFailure(String username, int lockMinutes) {
        long lockMillis = getLockMillis(lockMinutes);
        LoginState next = unknownAttempts.compute(username, (key, old) -> {
            long now = System.currentTimeMillis();
            if (old != null && old.getLockUntilMillis() > 0 && now < old.getLockUntilMillis()) {
                return old;
            }
            int failCount;
            if (old == null || (old.getLockUntilMillis() > 0 && now >= old.getLockUntilMillis())) {
                failCount = 1;
            } else {
                failCount = old.getFailCount() + 1;
            }
            long lockUntil = failCount >= MAX_ATTEMPTS ? now + lockMillis : 0L;
            return new LoginState(failCount, lockUntil);
        });
        boolean locked = next.getLockUntilMillis() > System.currentTimeMillis();
        int remaining = Math.max(0, MAX_ATTEMPTS - next.getFailCount());
        long remainingLockMinutes = locked ? getRemainingLockMinutes(username) : 0L;
        return new LoginResult(remaining, locked, lockMinutes, remainingLockMinutes);
    }
}
