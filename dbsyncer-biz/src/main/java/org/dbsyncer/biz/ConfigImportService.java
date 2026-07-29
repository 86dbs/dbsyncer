/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import java.io.File;

/**
 * 配置导入（支持 ZIP formatVersion=2 与旧版单体 JSON）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026/07/29
 */
public interface ConfigImportService {

    /**
     * 从文件导入配置；按后缀分发 ZIP / JSON。
     *
     * @param file 临时配置文件
     */
    void importConfig(File file);
}
