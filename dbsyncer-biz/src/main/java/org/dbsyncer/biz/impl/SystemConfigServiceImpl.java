/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ConfigExportService;
import org.dbsyncer.biz.ConfigImportService;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.UserConfigService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.vo.SystemConfigVO;
import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.enums.FileSuffixEnum;
import org.dbsyncer.common.enums.TaskLevelEnum;
import org.dbsyncer.common.model.ConfigModel;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.model.RsaVersion;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.RSAUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.parser.MetaProfile;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.TaskProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.io.File;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/17 23:20
 */
@Service
public class SystemConfigServiceImpl implements SystemConfigService {

    @Resource
    private ProfileComponent profileComponent;

    @Resource
    private MetaProfile metaProfile;

    @Resource
    private TaskProfile taskProfile;

    @Resource
    private PreloadTemplate preloadTemplate;

    @Resource
    private Checker systemConfigChecker;

    @Resource
    private Checker noticeConfigChecker;

    @Resource
    private UserConfigService userConfigService;

    @Resource
    private AppConfig appConfig;

    @Resource
    private ConfigImportService configImportService;

    @Resource
    private ConfigExportService configExportService;

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel model = systemConfigChecker.checkEditConfigModel(params);
        profileComponent.editConfigModel(model);
        return "修改成功.";
    }

    @Override
    public String editNoticeConfig(Map<String, String> params) {
        ConfigModel model = noticeConfigChecker.checkEditConfigModel(params);
        profileComponent.editConfigModel(model);
        preloadTemplate.loadNotificationChannel();
        return "修改成功.";
    }

    @Override
    public SystemConfigVO getSystemConfigVo() {
        SystemConfigVO systemConfigVo = new SystemConfigVO();
        BeanUtils.copyProperties(getSystemConfig(), systemConfigVo);
        systemConfigVo.setWatermark(getWatermark(systemConfigVo));
        return systemConfigVo;
    }

    @Override
    public SystemConfig getSystemConfig() {
        SystemConfig config = profileComponent.getSystemConfig();
        if (null != config) {
            return config;
        }

        synchronized (this) {
            config = profileComponent.getSystemConfig();
            if (null == config) {
                config = (SystemConfig) systemConfigChecker.checkAddConfigModel(new HashMap<>());
            }
            return config;
        }
    }

    @Override
    public List<ConfigModel> getConfigModelAll() {
        List<ConfigModel> list = new ArrayList<>();
        list.add(getSystemConfig());
        list.add(userConfigService.getUserConfig());
        list.addAll(profileComponent.getConnectorAll().stream().limit(5).collect(Collectors.toList()));
        Paging<Mapping> mappingPaging = taskProfile.queryTasks(Mapping.class, 1, 5, null);
        if (mappingPaging != null && !CollectionUtils.isEmpty(mappingPaging.getData())) {
            list.addAll(mappingPaging.getData());
        }
        Paging<Meta> metaPaging = metaProfile.queryMeta(TaskLevelEnum.TASK.getCode(), 1, 5);
        if (metaPaging != null && !CollectionUtils.isEmpty(metaPaging.getData())) {
            list.addAll(metaPaging.getData());
        }
        return list;
    }

    @Override
    public void checkFileSuffix(String filename) {
        Assert.hasText(filename, "the config filename is null.");
        String suffix = filename.substring(filename.lastIndexOf(".") + 1);
        FileSuffixEnum fileSuffix = FileSuffixEnum.getFileSuffix(suffix);
        Assert.notNull(fileSuffix, "Illegal file suffix");
        boolean supported = FileSuffixEnum.ZIP == fileSuffix;
        Assert.isTrue(supported, String.format("不正确的文件扩展名 \"%s\"，只支持 \"%s\" 的文件扩展名。",
                filename, FileSuffixEnum.ZIP.getName()));
    }

    @Override
    public void refreshConfig(File file) {
        configImportService.importConfig(file);
    }

    @Override
    public long estimateExportSize() {
        return configExportService.estimateExportSize();
    }

    @Override
    public String getWatermark(SystemConfig systemConfig) {
        return StringUtil.isNotBlank(systemConfig.getWatermark()) ? systemConfig.getWatermark() : appConfig.getName() + "-${username}<br />" + appConfig.getCompany();
    }

    @Override
    public RsaVersion createRSAConfig(int keyLength) {
        Assert.isTrue(keyLength >= 1024 && keyLength <= 8192, "密钥长度支持的范围[1024-8192]");
        return RSAUtil.createKeys(keyLength);
    }

    @Override
    public String generateApiSecret() {
        byte[] bytes = new byte[32];
        new SecureRandom().nextBytes(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }
}