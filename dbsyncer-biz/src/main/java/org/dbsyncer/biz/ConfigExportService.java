/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.common.model.VersionInfo;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 配置导出（流式 ZIP）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026/07/29
 */
public interface ConfigExportService {

    /**
     * 流式写出配置 ZIP 包到输出流（边查边写，不全量进内存）。
     *
     * @param out         目标流（如 HttpServletResponse 输出流）
     * @param versionInfo 版本头信息
     * @throws IOException 写出失败
     */
    void exportZip(OutputStream out, VersionInfo versionInfo) throws IOException;

    /**
     * 按行数粗估导出未压缩体积（字节），供列表页提示，避免全量序列化。
     *
     * @return 估算字节数
     */
    long estimateExportSize();
}
