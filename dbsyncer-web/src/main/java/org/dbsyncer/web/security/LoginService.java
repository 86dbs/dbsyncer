/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.security;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.model.UserInfo;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;

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

    /**
     * 锁定时长（毫秒）
     */
    public static final long LOCK_MILLIS = 15 * 60 * 1000L;

    @Resource
    private UserProfile userProfile;

    /**
     * 未知用户名的进程内失败计数（不落库）
     */
    private final ConcurrentHashMap<String, LoginState> unknownAttempts = new ConcurrentHashMap<>();

    /**
     * 账号是否处于锁定中（过期则清零并返回 false）。
     */
    public boolean isLocked(String username) {
        if (StringUtil.isBlank(username)) {
            return false;
        }
        UserInfo user = userProfile.getByUsername(username);
        if (user == null) {
            return isUnknownLocked(username);
        }
        long lockUntil = user.getLockUntil();
        if (lockUntil <= 0) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (now < lockUntil) {
            return true;
        }
        userProfile.updateLoginLock(username, 0, 0L);
        return false;
    }

    /**
     * 记录一次登录失败；达到阈值则锁定。
     *
     * @return 调用后是否处于锁定状态
     */
    public boolean onFailure(String username) {
        if (StringUtil.isBlank(username)) {
            return false;
        }
        Boolean locked = userProfile.recordLoginFailure(username, MAX_ATTEMPTS, LOCK_MILLIS);
        if (locked == null) {
            return onUnknownFailure(username);
        }
        return locked;
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

    private boolean isUnknownLocked(String username) {
        LoginState state = unknownAttempts.get(username);
        if (state == null) {
            return false;
        }
        long lockUntil = state.getLockUntilMillis();
        if (lockUntil <= 0) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (now < lockUntil) {
            return true;
        }
        unknownAttempts.remove(username, state);
        return false;
    }

    private boolean onUnknownFailure(String username) {
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
            long lockUntil = failCount >= MAX_ATTEMPTS ? now + LOCK_MILLIS : 0L;
            return new LoginState(failCount, lockUntil);
        });
        return next.getLockUntilMillis() > System.currentTimeMillis();
    }
}
