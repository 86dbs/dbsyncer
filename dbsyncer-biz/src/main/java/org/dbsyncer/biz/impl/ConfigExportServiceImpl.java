/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConfigExportService;
import org.dbsyncer.common.config.PackageFormatConfig;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.model.VersionInfo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.impl.OperationTemplate;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.SortEnum;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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
    private OperationTemplate operationTemplate;

    @Resource
    private StorageService storageService;

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
            counts.put(ConfigConstant.SYSTEM, writeJsonArray(zos, PackageFormatConfig.SYSTEM, operationTemplate.queryAll(SystemConfig.class)));
            counts.put(ConfigConstant.USER, writeJsonArray(zos, PackageFormatConfig.USER, operationTemplate.queryAll(UserConfig.class)));
            counts.put(ConfigConstant.CONNECTOR, writeJsonArray(zos, PackageFormatConfig.CONNECTOR, operationTemplate.queryAll(Connector.class)));
            List<String> taskIds = new ArrayList<>();
            counts.put(ConfigConstant.TASK, writeAllTasks(zos, taskIds));
            counts.put(ConfigConstant.TABLE_GROUP, writeTableGroups(zos));
            counts.put(ConfigConstant.META, writeJsonArray(zos, PackageFormatConfig.META, operationTemplate.queryAll(Meta.class)));
            counts.put(StorageEnum.TASK_DETAIL.getType(), writeTaskDetailSchemas(zos, taskIds));
            writeManifest(zos, versionInfo, counts);
        }
    }

    @Override
    public long estimateExportSize() {
        long rows = 0L;
        rows += operationTemplate.count(StorageEnum.CONFIG, null);
        rows += operationTemplate.count(StorageEnum.USER, null);
        rows += operationTemplate.count(StorageEnum.CONNECTOR, null);
        rows += operationTemplate.count(StorageEnum.TASK, null);
        rows += operationTemplate.count(StorageEnum.TABLE_GROUP, null);
        rows += operationTemplate.count(StorageEnum.META, null);
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
        List<Object> tasks = new ArrayList<>();
        Query query = new Query();
        query.setType(StorageEnum.TASK);
        query.setPageSize(ConfigConstant.PAGE_SIZE);
        while (true) {
            Paging paging = storageService.query(query);
            if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                break;
            }
            List<Map> data = (List<Map>) paging.getData();
            for (Map row : data) {
                Object json = row.get(ConfigConstant.CONFIG_MODEL_JSON);
                if (json == null) {
                    continue;
                }
                Map task = JsonUtil.parseMap(String.valueOf(json));
                if (task != null) {
                    tasks.add(task);
                    Object id = task.get(ConfigConstant.CONFIG_MODEL_ID);
                    if (id != null && StringUtil.isNotBlank(String.valueOf(id))) {
                        taskIds.add(String.valueOf(id));
                    }
                }
            }
            query.setPageNum(query.getPageNum() + 1);
        }
        writeBytes(zos, PackageFormatConfig.TASK, JsonUtil.objToJson(tasks).getBytes(StandardCharsets.UTF_8));
        return tasks.size();
    }

    /**
     * 导出 task_detail 动态分表结构清单（仅 taskIds，不导出行数据）。
     */
    private int writeTaskDetailSchemas(ZipOutputStream zos, List<String> taskIds) throws IOException {
        List<String> ids = taskIds == null ? Collections.emptyList() : taskIds;
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("taskIds", ids);
        writeBytes(zos, PackageFormatConfig.TASK_DETAIL, JsonUtil.objToJson(payload).getBytes(StandardCharsets.UTF_8));
        return ids.size();
    }

    private int writeTableGroups(ZipOutputStream zos) throws IOException {
        Query query = new Query();
        query.setType(StorageEnum.TABLE_GROUP);
        query.setPageSize(ConfigConstant.PAGE_SIZE);
        query.addOrderBy(ConfigConstant.TABLE_GROUP_TASK_ID, SortEnum.ASC);

        String currentTaskId = null;
        BufferedWriter writer = null;
        int count = 0;
        try {
            while (true) {
                Paging paging = storageService.query(query);
                if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
                    break;
                }
                List<Map> data = (List<Map>) paging.getData();
                for (Map row : data) {
                    TableGroup tg = ConfigModelUtil.parseFromRow(row, TableGroup.class);
                    if (tg == null || StringUtil.isBlank(tg.getTaskId())) {
                        continue;
                    }
                    if (!StringUtil.equals(currentTaskId, tg.getTaskId())) {
                        flushWriter(writer);
                        if (currentTaskId != null) {
                            zos.closeEntry();
                        }
                        currentTaskId = tg.getTaskId();
                        zos.putNextEntry(new ZipEntry(PackageFormatConfig.TABLE_GROUP_DIR + currentTaskId + PackageFormatConfig.NDJSON_SUFFIX));
                        writer = new BufferedWriter(new OutputStreamWriter(zos, StandardCharsets.UTF_8));
                    }
                    writer.write(JsonUtil.objToJson(tg));
                    writer.newLine();
                    count++;
                }
                query.setPageNum(query.getPageNum() + 1);
            }
        } finally {
            flushWriter(writer);
            if (currentTaskId != null) {
                zos.closeEntry();
            }
        }
        return count;
    }

    private void flushWriter(BufferedWriter writer) throws IOException {
        if (writer != null) {
            writer.flush();
        }
    }

    private void writeBytes(ZipOutputStream zos, String entryName, byte[] bytes) throws IOException {
        zos.putNextEntry(new ZipEntry(entryName));
        zos.write(bytes);
        zos.closeEntry();
    }
}
