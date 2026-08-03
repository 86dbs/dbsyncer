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
     * 按账号同步增删改用户行（替代原 OperationTemplate.syncUserConfig）。
     *
     * @return 首个用户 id（兼容旧返回值）
     */
    String syncUserConfig(UserConfig config);
}
