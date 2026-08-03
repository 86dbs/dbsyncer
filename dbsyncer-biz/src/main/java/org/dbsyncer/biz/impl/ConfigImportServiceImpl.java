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
import org.dbsyncer.common.util.PackageZipUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.parser.ConnectorProfile;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.parser.TableGroupProfile;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.UserProfile;
import org.dbsyncer.parser.model.TaskImportResult;
import org.dbsyncer.sdk.spi.TaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.ZipFile;

/**
 * 配置导入：ZIP(formatVersion=2) 委托各 Profile；旧版单体 JSON 走 PreloadTemplate。
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
            VersionInfo info = PackageZipUtil.validateManifest(zip);
            logger.info("upload config zip: appName={}, version={}, createTime={}",
                    info.getAppName(), info.getVersion(), info.getCreateTime());

            systemConfigProfile.importFromJson(PackageZipUtil.readOptionalEntry(zip, PackageFormatConfig.SYSTEM));
            userProfile.importFromJson(PackageZipUtil.readOptionalEntry(zip, PackageFormatConfig.USER));
            connectorProfile.importConnectorsFromJson(PackageZipUtil.readOptionalEntry(zip, PackageFormatConfig.CONNECTOR));

            TaskImportResult taskResult = taskProfile.importTasksFromZip(zip);
            for (ConfigModel task : taskResult.getEnterpriseTasks()) {
                taskService.edit(task);
            }

            tableGroupProfile.importFromZip(zip);
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
}
