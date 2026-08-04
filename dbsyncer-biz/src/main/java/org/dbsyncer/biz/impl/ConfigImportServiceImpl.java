/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.apache.commons.io.FileUtils;
import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.ConfigImportService;
import org.dbsyncer.common.config.PackageFormatConfig;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.VersionInfo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.PackageZipUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.parser.ConnectorProfile;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.TaskImportResult;
import org.dbsyncer.sdk.spi.TaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipFile;

/**
 * 配置导入：ZIP(formatVersion=2) 委托各 Profile 落库，再经 PreloadTemplate 收尾启停。
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
    private SystemConfigProfile systemConfigProfile;

    @Resource
    private UserProfile userProfile;

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private ConnectorProfile connectorProfile;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private TableGroupProfile tableGroupProfile;

    @Resource
    private TaskService<ConfigModel> taskService;

    @Override
    public void importConfig(File file) {
        Assert.notNull(file, "the config file is null.");
        String name = file.getName();
        Assert.isTrue(StringUtil.isNotBlank(name) && name.toLowerCase().endsWith(".zip"),
                "仅支持导入 .zip 配置包");
        try {
            importZip(file);
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    private void importZip(File file) {
        try (ZipFile zip = new ZipFile(file, StandardCharsets.UTF_8)) {
            VersionInfo info = PackageZipUtil.validateManifest(zip);
            logger.info("upload config zip: appName={}, version={}, createTime={}",
                    info.getAppName(), info.getVersion(), info.getCreateTime());

            systemConfigProfile.importFromJson(PackageZipUtil.readOptionalEntry(zip, PackageFormatConfig.SYSTEM));
            ensureSystemConfig();
            userProfile.importFromJson(PackageZipUtil.readOptionalEntry(zip, PackageFormatConfig.USER));
            connectorProfile.importConnectorsFromJson(PackageZipUtil.readOptionalEntry(zip, PackageFormatConfig.CONNECTOR));

            TaskImportResult taskResult = taskProfile.importTasksFromZip(zip);
            for (ConfigModel task : taskResult.getEnterpriseTasks()) {
                taskService.edit(task);
            }

            importTableGroupsFromZip(zip);
            metaProfile.importMetaFromJson(PackageZipUtil.readOptionalEntry(zip, PackageFormatConfig.META));
            taskProfile.importTaskDetailSchemasFromJson(PackageZipUtil.readOptionalEntry(zip, PackageFormatConfig.TASK_DETAIL));

            preloadTemplate.afterConfigImport();
        } catch (BizException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw new BizException(e.getMessage(), e);
        } catch (Exception e) {
            throw new BizException("导入配置 ZIP 失败: " + e.getMessage(), e);
        }
    }

    /**
     * 导入后若无系统配置则补默认行，避免增量启动等路径 getSystemConfig() 为空 NPE。
     */
    private void ensureSystemConfig() {
        if (systemConfigProfile.getSystemConfig() != null) {
            return;
        }
        SystemConfig config = new SystemConfig();
        config.setName("系统配置");
        long now = System.currentTimeMillis();
        config.setCreateTime(now);
        config.setUpdateTime(now);
        systemConfigProfile.saveSystemConfig(config);
        logger.warn("Imported package has no system config, created default system config");
    }

    /**
     * 从 ZIP 导入 table_group/*.ndjson（不预建表级 Meta，Meta 由后续 meta.json 还原）。
     */
    private void importTableGroupsFromZip(ZipFile zip) throws IOException {
        if (zip == null) {
            return;
        }
        List<String> buffer = new ArrayList<>(PackageFormatConfig.IMPORT_BATCH_SIZE);
        PackageZipUtil.pageScanTableGroupNdjsonLines(zip, line -> {
            buffer.add(line);
            if (buffer.size() >= PackageFormatConfig.IMPORT_BATCH_SIZE) {
                importTableGroupNdjsonLines(new ArrayList<>(buffer));
                buffer.clear();
            }
        });
        if (!CollectionUtils.isEmpty(buffer)) {
            importTableGroupNdjsonLines(buffer);
        }
    }

    private void importTableGroupNdjsonLines(List<String> ndjsonLines) {
        if (CollectionUtils.isEmpty(ndjsonLines)) {
            return;
        }
        List<TableGroup> buffer = new ArrayList<>(PackageFormatConfig.IMPORT_BATCH_SIZE);
        for (String line : ndjsonLines) {
            if (StringUtil.isBlank(line)) {
                continue;
            }
            TableGroup tg = JsonUtil.jsonToObj(line, TableGroup.class);
            if (tg == null) {
                continue;
            }
            buffer.add(tg);
            if (buffer.size() >= PackageFormatConfig.IMPORT_BATCH_SIZE) {
                tableGroupProfile.addTableGroupBatchWithoutMeta(new ArrayList<>(buffer));
                buffer.clear();
            }
        }
        if (!CollectionUtils.isEmpty(buffer)) {
            tableGroupProfile.addTableGroupBatchWithoutMeta(new ArrayList<>(buffer));
        }
    }
}
