/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.AppConfigService;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.vo.VersionVO;
import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.SystemConfig;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-05-12 01:08
 */
@Component
public class AppConfigServiceImpl implements AppConfigService {

    @Resource
    private AppConfig appConfig;

    @Resource
    private SystemConfigService systemConfigService;

    public static final String WATERMARK_USERNAME = "${username}";

    @Override
    public VersionVO getVersionInfo(String username) {
        VersionVO versionVo = new VersionVO(appConfig.getName(), appConfig.getCopyright());
        // 是否启用水印
        SystemConfig systemConfig = systemConfigService.getSystemConfig();
        if (systemConfig.isEnableWatermark()) {
            String watermark = systemConfigService.getWatermark(systemConfig);
            watermark = StringUtil.replace(watermark, WATERMARK_USERNAME, username);
            versionVo.setWatermark(watermark);
        }
        return versionVo;
    }
}