/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.model;

import org.dbsyncer.common.model.ConfigModel;

import java.util.Collections;
import java.util.List;

/**
 * {@link org.dbsyncer.parser.TaskProfile#importTasksFromJson(String)} 结果。
 *
 * @author wuji
 * @version 1.0.0
 */
public final class TaskImportResult {

    private final int mappingCount;

    private final List<ConfigModel> enterpriseTasks;

    public TaskImportResult(int mappingCount, List<ConfigModel> enterpriseTasks) {
        this.mappingCount = mappingCount;
        this.enterpriseTasks = enterpriseTasks == null ? Collections.emptyList() : enterpriseTasks;
    }

    public int getMappingCount() {
        return mappingCount;
    }

    /**
     * 已落库的企业任务（校验/迁移），需由调用方刷新 TaskService 内存缓存。
     */
    public List<ConfigModel> getEnterpriseTasks() {
        return enterpriseTasks;
    }
}
