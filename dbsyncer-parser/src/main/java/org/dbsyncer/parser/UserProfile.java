/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.parser.model.UserInfo;

import java.util.List;

/**
 * 用户配置（一行一用户落库；聚合模型 {@link UserConfig} 供 UI/导入导出）。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface UserProfile {

    /**
     * 获取聚合用户配置（无用户时返回 null）。
     */
    UserConfig getUserConfig();

    /**
     * 列出全部用户行。
     */
    List<UserInfo> listUserInfos();

    /**
     * 按账号查询用户行。
     */
    UserInfo getByUsername(String username);

    /**
     * 按用户 ID 查询用户行。
     */
    UserInfo getById(String id);

    /**
     * 按账号同步增删改用户行（替代原 OperationTemplate.syncUserConfig）。
     *
     * @return 首个用户 id（兼容旧返回值）
     */
    String saveUserConfig(UserConfig config);

    /**
     * 用户行数。
     */
    int countUsers();

    /**
     * 从 user.json 数组导入。
     */
    void importFromJson(String json);

    /**
     * 更新登录失败次数与锁定截止时间（单用户行）。
     *
     * @param username       账号
     * @param loginFailCount 失败次数
     * @param lockUntil      锁定截止毫秒时间戳，0 表示未锁定
     * @return 是否找到并更新
     */
    boolean updateLoginLock(String username, int loginFailCount, long lockUntil);

    /**
     * 记录一次登录失败；达阈值则写入锁定截止时间。
     *
     * @param username    账号
     * @param maxAttempts 连续失败上限
     * @param lockMillis  锁定时长（毫秒）
     * @return null 表示用户不存在；否则表示更新后是否处于锁定
     */
    Boolean recordLoginFailure(String username, int maxAttempts, long lockMillis);
}
