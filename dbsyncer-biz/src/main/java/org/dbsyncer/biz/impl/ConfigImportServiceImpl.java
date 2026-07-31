/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConfigImportService;
import org.dbsyncer.common.config.PackageFormatConfig;
import org.dbsyncer.common.enums.CommonTaskTypeEnum;
import org.dbsyncer.common.model.VersionInfo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.TaskSplitUtil;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.impl.OperationTemplate;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.OperationConfig;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.model.CommonTask;
import org.dbsyncer.sdk.model.DatabaseSyncTask;
import org.dbsyncer.sdk.model.ValidateSyncTask;
import org.dbsyncer.sdk.spi.TaskService;
import org.dbsyncer.sdk.storage.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * 配置导入：ZIP(formatVersion=2) 按分片批量落库；旧版单体 JSON 走
 * task.json 含同步 / 订正校验 / 整库迁移；兼容旧 ZIP 的 mapping.json。
 *
 * @author AE86
 * @version 1.0.0
 * @date 2026/07/29
 */
@Service
public class ConfigImportServiceImpl implements ConfigImportService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private PreloadTemplate preloadTemplate;

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private TaskService<CommonTask> taskService;

    @Resource
    private StorageService storageService;

    @Override
    public void importConfig(File file) {
        Assert.notNull(file, "the config file is null.");
        String name = file.getName();
        try {
            if (StringUtil.isNotBlank(name) && name.toLowerCase().endsWith(".zip")) {
                importZip(file);
            } else {
                importJson(file);
            }
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    private void importJson(File file) {
        try {
            List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
            if (CollectionUtils.isEmpty(lines)) {
                return;
            }
            StringBuilder json = new StringBuilder();
            lines.forEach(json::append);
            preloadTemplate.reload(json.toString());
        } catch (IOException e) {
            throw new BizException("读取配置 JSON 失败", e);
        }
    }

    private void importZip(File file) {
        try (ZipFile zip = new ZipFile(file, StandardCharsets.UTF_8)) {
            validateManifest(zip);
            // 依赖顺序：system → user → connector → task → table_group → meta → task_detail 空表
            importJsonModels(zip, PackageFormatConfig.SYSTEM, SystemConfig.class, false);
            importJsonModels(zip, PackageFormatConfig.USER, UserConfig.class, false);
            importConnectors(zip);
            importTasks(zip);
            importTableGroups(zip);
            importJsonModels(zip, PackageFormatConfig.META, Meta.class, true);
            importTaskDetailSchemas(zip);
            preloadTemplate.afterConfigImport();
        } catch (BizException e) {
            throw e;
        } catch (Exception e) {
            throw new BizException("导入配置 ZIP 失败: " + e.getMessage(), e);
        }
    }

    private void validateManifest(ZipFile zip) throws IOException {
        ZipEntry entry = zip.getEntry(PackageFormatConfig.MANIFEST);
        Assert.notNull(entry, "不支持导入低版本或配置不完整：缺少 manifest.json");
        String json = readEntryAsString(zip, entry);
        Map map = JsonUtil.parseMap(json);
        Assert.notNull(map, "manifest.json 无效");
        Object formatVersion = map.get("formatVersion");
        Assert.isTrue(formatVersion != null, "不支持导入低版本或配置不完整");
        VersionInfo info = new VersionInfo();
        Object version = map.get("version");
        if (version instanceof Number) {
            info.setVersion(((Number) version).longValue());
        }
        Object appName = map.get("appName");
        if (appName != null) {
            info.setAppName(String.valueOf(appName));
        }
        Object createTime = map.get("createTime");
        if (createTime instanceof Number) {
            info.setCreateTime(((Number) createTime).longValue());
        }
        logger.info("upload config zip: appName={}, version={}, formatVersion={}, createTime={}",
                info.getAppName(), info.getVersion(), formatVersion, info.getCreateTime());
    }

    private <T extends ConfigModel> void importJsonModels(ZipFile zip, String entryName, Class<T> clazz, boolean batch) throws IOException {
        ZipEntry entry = zip.getEntry(entryName);
        if (entry == null) {
            return;
        }
        String json = readEntryAsString(zip, entry);
        if (StringUtil.isBlank(json)) {
            return;
        }
        List<T> models = JsonUtil.jsonToArray(json, clazz);
        if (CollectionUtils.isEmpty(models)) {
            return;
        }
        if (!batch || models.size() == 1) {
            for (T model : models) {
                operationTemplate.execute(new OperationConfig(model, CommandEnum.OPR_ADD));
            }
            return;
        }
        TaskSplitUtil.split(models, PackageFormatConfig.IMPORT_BATCH_SIZE, batchList ->
                operationTemplate.executeBatch(batchList, CommandEnum.OPR_ADD));
    }

    private void importConnectors(ZipFile zip) throws IOException {
        ZipEntry entry = zip.getEntry(PackageFormatConfig.CONNECTOR);
        if (entry == null) {
            return;
        }
        String json = readEntryAsString(zip, entry);
        if (StringUtil.isBlank(json)) {
            return;
        }
        List list = JsonUtil.parseList(json);
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        List<Connector> connectors = new ArrayList<>(list.size());
        for (Object item : list) {
            Connector connector = profileComponent.parseConnector(JsonUtil.objToJson(item));
            if (connector != null) {
                connectors.add(connector);
            }
        }
        TaskSplitUtil.split(connectors, PackageFormatConfig.IMPORT_BATCH_SIZE, batch ->
                operationTemplate.executeBatch(batch, CommandEnum.OPR_ADD));
    }

    /**
     * 导入任务表：优先 task.json（全类型），兼容旧 ZIP 的 mapping.json（仅同步任务）。
     */
    private void importTasks(ZipFile zip) throws IOException {
        ZipEntry entry = zip.getEntry(PackageFormatConfig.TASK);
        if (entry == null) {
            entry = zip.getEntry(PackageFormatConfig.MAPPING);
        }
        if (entry == null) {
            return;
        }
        String json = readEntryAsString(zip, entry);
        if (StringUtil.isBlank(json)) {
            return;
        }
        List list = JsonUtil.parseList(json);
        if (CollectionUtils.isEmpty(list)) {
            return;
        }

        List<Mapping> mappings = new ArrayList<>();
        for (Object item : list) {
            Map map = item instanceof Map ? (Map) item : JsonUtil.parseMap(JsonUtil.objToJson(item));
            if (map == null) {
                continue;
            }
            String type = map.get(ConfigConstant.CONFIG_MODEL_TYPE) == null
                    ? null : String.valueOf(map.get(ConfigConstant.CONFIG_MODEL_TYPE));
            String itemJson = JsonUtil.objToJson(map);
            if (StringUtil.equals(ConfigConstant.MAPPING, type) || StringUtil.isBlank(type)) {
                // 旧 mapping.json 无 type 或 type=mapping
                Mapping mapping = profileComponent.parseObject(itemJson, Mapping.class);
                if (mapping != null) {
                    if (StringUtil.isBlank(mapping.getType())) {
                        mapping.setType(ConfigConstant.MAPPING);
                    }
                    mappings.add(mapping);
                }
                continue;
            }
            CommonTaskTypeEnum taskType = CommonTaskTypeEnum.parse(type);
            if (taskType == CommonTaskTypeEnum.VALIDATE_SYNC) {
                ValidateSyncTask task = JsonUtil.jsonToObj(itemJson, ValidateSyncTask.class);
                if (task != null) {
                    persistEnterpriseTask(task);
                }
            } else if (taskType == CommonTaskTypeEnum.DATABASE_SYNC) {
                DatabaseSyncTask task = JsonUtil.jsonToObj(itemJson, DatabaseSyncTask.class);
                if (task != null) {
                    persistEnterpriseTask(task);
                }
            } else {
                logger.warn("跳过未知任务类型: type={}, id={}", type, map.get(ConfigConstant.CONFIG_MODEL_ID));
            }
        }
        TaskSplitUtil.split(mappings, PackageFormatConfig.IMPORT_BATCH_SIZE, batch ->
                operationTemplate.executeBatch(batch, CommandEnum.OPR_ADD));
    }

    /**
     * 企业任务落库：直接写 dbsyncer_task，再用 edit 刷新 TaskService 内存缓存（不走 add，避免重复建 Meta）。
     */
    private void persistEnterpriseTask(CommonTask task) {
        Map<String, Object> params = new HashMap<>();
        params.put(ConfigConstant.CONFIG_MODEL_ID, task.getId());
        params.put(ConfigConstant.CONFIG_MODEL_NAME, task.getName());
        params.put(ConfigConstant.CONFIG_MODEL_TYPE, task.getType());
        params.put(ConfigConstant.CONFIG_MODEL_JSON, JsonUtil.objToJson(task));
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, task.getCreateTime());
        params.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, task.getUpdateTime());
        storageService.add(StorageEnum.TASK, params);
        // 企业 TaskServiceImpl.edit 会 saveTaskAndCache；开源桩为 no-op
        taskService.edit(task);
    }

    private void importTableGroups(ZipFile zip) throws IOException {
        Enumeration<? extends ZipEntry> entries = zip.entries();
        List<TableGroup> buffer = new ArrayList<>(PackageFormatConfig.IMPORT_BATCH_SIZE);
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            String name = entry.getName();
            if (entry.isDirectory() || !name.startsWith(PackageFormatConfig.TABLE_GROUP_DIR)
                    || !name.endsWith(PackageFormatConfig.NDJSON_SUFFIX)) {
                continue;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(zip.getInputStream(entry), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (StringUtil.isBlank(line)) {
                        continue;
                    }
                    TableGroup tg = profileComponent.parseObject(line, TableGroup.class);
                    if (tg == null) {
                        continue;
                    }
                    buffer.add(tg);
                    if (buffer.size() >= PackageFormatConfig.IMPORT_BATCH_SIZE) {
                        flushTableGroups(buffer);
                    }
                }
            }
        }
        flushTableGroups(buffer);
    }

    private void flushTableGroups(List<TableGroup> buffer) {
        if (CollectionUtils.isEmpty(buffer)) {
            return;
        }
        operationTemplate.executeBatch(new ArrayList<>(buffer), CommandEnum.OPR_ADD);
        buffer.clear();
    }

    /**
     * 按 ZIP 中 task_detail.json 的 taskIds 预建空分表（仅结构，无行数据）。
     * 兼容旧包：无该文件时跳过。
     */
    private void importTaskDetailSchemas(ZipFile zip) throws IOException {
        ZipEntry entry = zip.getEntry(PackageFormatConfig.TASK_DETAIL);
        if (entry == null) {
            return;
        }
        String json = readEntryAsString(zip, entry);
        if (StringUtil.isBlank(json)) {
            return;
        }
        Map map = JsonUtil.parseMap(json);
        if (map == null) {
            return;
        }
        Object taskIdsObj = map.get("taskIds");
        if (!(taskIdsObj instanceof List)) {
            return;
        }
        for (Object item : (List) taskIdsObj) {
            if (item == null) {
                continue;
            }
            String taskId = String.valueOf(item);
            if (StringUtil.isBlank(taskId)) {
                continue;
            }
            storageService.ensure(StorageEnum.TASK_DETAIL, taskId);
        }
    }

    private String readEntryAsString(ZipFile zip, ZipEntry entry) throws IOException {
        try (InputStream in = zip.getInputStream(entry)) {
            return new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8);
        }
    }
}
