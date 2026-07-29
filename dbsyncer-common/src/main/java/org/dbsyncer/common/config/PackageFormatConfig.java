/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.common.config;

/**
 * 配置导入导出 ZIP 包格式约定（formatVersion=2）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026/07/29
 */
public final class PackageFormatConfig {

    /**
     * 新 ZIP 分片格式版本号
     */
    public static final int FORMAT_VERSION = 2;

    public static final String MANIFEST = "manifest.json";
    public static final String SYSTEM = "system.json";
    public static final String USER = "user.json";
    public static final String CONNECTOR = "connector.json";
    /**
     * 任务表全量（同步 mapping / 订正校验 / 整库迁移）
     */
    public static final String TASK = "task.json";
    /**
     * 兼容旧 ZIP：仅含同步任务
     */
    public static final String MAPPING = "mapping.json";
    public static final String META = "meta.json";
    public static final String TABLE_GROUP_DIR = "table_group/";
    public static final String NDJSON_SUFFIX = ".ndjson";

    /**
     * 导入时 table_group / task 等批量写库批次大小
     */
    public static final int IMPORT_BATCH_SIZE = 200;

    /**
     * 列表页导出体积粗估：每条配置约占用字节（未压缩）
     */
    public static final long ESTIMATE_BYTES_PER_ROW = 2048L;

    private PackageFormatConfig() {
    }
}
