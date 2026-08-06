/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import java.io.File;

/**
 * 配置导入（ZIP formatVersion=2）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026/07/29
 */
public interface ConfigImportService {

    /**
     * 从 ZIP 配置包导入。
     *
     * @param file 临时配置文件
     */
    void importConfig(File file);
}
