/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.model.SystemConfig;

/**
 * 系统配置（{@code dbsyncer_config}）读写。
 *
 * @author wuji
 * @version 1.0.0
 */
public interface SystemConfigProfile {

    /**
     * 获取系统配置（无则 null）。
     */
    SystemConfig getSystemConfig();

    /**
     * 保存系统配置（新增或更新）。
     */
    String saveSystemConfig(SystemConfig config);

    /**
     * 系统配置行数。
     */
    int countSystemConfigs();

    /**
     * 从 system.json 数组导入。
     */
    void importFromJson(String json);

    /**
     * 删除系统配置。
     */
    void removeSystemConfig(String id);
}
