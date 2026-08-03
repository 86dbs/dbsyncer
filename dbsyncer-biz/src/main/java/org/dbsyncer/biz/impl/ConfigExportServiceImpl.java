/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConfigExportService;
import org.dbsyncer.common.config.PackageFormatConfig;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.VersionInfo;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ConnectorProfile;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * 流式 ZIP 配置导出：system/user/connector/task/meta 写 JSON 数组，
 * table_group 按 taskId 分文件写 NDJSON；task.json 含同步/校验/迁移全量任务；
 * task_detail.json 仅含任务 ID 清单，导入时预建空分表（不导出行数据）。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/07/29
 */
@Service
public class ConfigExportServiceImpl implements ConfigExportService {

    @Resource
    private UserProfile userProfile;

    @Resource
    private ConnectorProfile connectorProfile;

    @Resource
    private SystemConfigProfile systemConfigProfile;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Override
    public void exportZip(OutputStream out, VersionInfo versionInfo) throws IOException {
        if (out == null) {
            throw new BizException("导出输出流不能为空");
        }
        if (versionInfo == null) {
            throw new BizException("版本信息不能为空");
        }

        Map<String, Integer> counts = new LinkedHashMap<>();
        try (ZipOutputStream zos = new ZipOutputStream(out)) {
            SystemConfig systemConfig = systemConfigProfile.getSystemConfig();
            counts.put(ConfigConstant.SYSTEM, writeJsonArray(zos, PackageFormatConfig.SYSTEM,
                    systemConfig == null ? Collections.emptyList() : Collections.singletonList(systemConfig)));
            UserConfig userConfig = userProfile.getUserConfig();
            counts.put(ConfigConstant.USER, writeJsonArray(zos, PackageFormatConfig.USER,
                    userConfig == null ? Collections.emptyList() : Collections.singletonList(userConfig)));
            counts.put(ConfigConstant.CONNECTOR, writeJsonArray(zos, PackageFormatConfig.CONNECTOR, connectorProfile.getConnectorAll()));
            List<String> taskIds = new ArrayList<>();
            counts.put(ConfigConstant.TASK, writeAllTasks(zos, taskIds));
            counts.put(ConfigConstant.TABLE_GROUP, tableGroupProfile.writeTableGroupsToZip(zos));
            counts.put(ConfigConstant.META, writeJsonArray(zos, PackageFormatConfig.META, metaProfile.getMetaAll()));
            counts.put(StorageEnum.TASK_DETAIL.getType(), writeTaskDetailSchemas(zos, taskIds));
            writeManifest(zos, versionInfo, counts);
        }
    }

    @Override
    public long estimateExportSize() {
        long rows = 0L;
        rows += systemConfigProfile.countSystemConfigs();
        rows += userProfile.countUsers();
        rows += connectorProfile.countConnectors();
        rows += taskProfile.countAllTasks();
        rows += tableGroupProfile.countTableGroups();
        rows += metaProfile.countMeta();
        return Math.max(rows, 1L) * PackageFormatConfig.ESTIMATE_BYTES_PER_ROW;
    }

    private void writeManifest(ZipOutputStream zos, VersionInfo versionInfo, Map<String, Integer> counts) throws IOException {
        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("formatVersion", PackageFormatConfig.FORMAT_VERSION);
        manifest.put("version", versionInfo.getVersion());
        manifest.put("appName", versionInfo.getAppName());
        manifest.put("createTime", versionInfo.getCreateTime());
        manifest.put("counts", counts);
        writeBytes(zos, PackageFormatConfig.MANIFEST, JsonUtil.objToJson(manifest).getBytes(StandardCharsets.UTF_8));
    }

    private int writeJsonArray(ZipOutputStream zos, String entryName, List<? extends ConfigModel> models) throws IOException {
        List<? extends ConfigModel> list = models == null ? Collections.emptyList() : models;
        writeBytes(zos, entryName, JsonUtil.objToJson(list).getBytes(StandardCharsets.UTF_8));
        return list.size();
    }

    /**
     * 导出 dbsyncer_task 全表：同步(mapping)、订正校验(VALIDATE_SYNC)、整库迁移(DATABASE_SYNC)。
     *
     * @param taskIds 收集任务 ID，供导出 task_detail 分表结构清单
     */
    private int writeAllTasks(ZipOutputStream zos, List<String> taskIds) throws IOException {
        List<Map<String, Object>> tasks = taskProfile.listAllTaskJsonMaps();
        for (Map<String, Object> task : tasks) {
            Object id = task.get(ConfigConstant.CONFIG_MODEL_ID);
            if (id != null && StringUtil.isNotBlank(String.valueOf(id))) {
                taskIds.add(String.valueOf(id));
            }
        }
        writeBytes(zos, PackageFormatConfig.TASK, JsonUtil.objToJson(tasks).getBytes(StandardCharsets.UTF_8));
        return tasks.size();
    }

    /**
     * 导出 task_detail 动态分表结构清单（仅 taskIds，不导出行数据）。
     */
    private int writeTaskDetailSchemas(ZipOutputStream zos, List<String> taskIds) throws IOException {
        writeBytes(zos, PackageFormatConfig.TASK_DETAIL,
                taskProfile.exportTaskDetailSchemasJson(taskIds).getBytes(StandardCharsets.UTF_8));
        return taskIds == null ? 0 : taskIds.size();
    }

    private void writeBytes(ZipOutputStream zos, String entryName, byte[] bytes) throws IOException {
        zos.putNextEntry(new ZipEntry(entryName));
        zos.write(bytes);
        zos.closeEntry();
    }
}
