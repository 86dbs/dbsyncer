/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.SystemConfigVo;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.SystemConfig;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * 系统配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/5/30 23:14
 */
public interface SystemConfigService {

    /**
     * 修改系统配置
     *
     * @param params
     */
    String edit(Map<String, String> params);

    /**
     * 获取系统配置
     *
     * @return
     */
    SystemConfigVo getSystemConfigVo();

    /**
     * 获取系统配置
     *
     * @return
     */
    SystemConfig getSystemConfig();

    /**
     * 获取所有配置（system、user、connector、mapping、tableGroup、meta、projectGroup）
     *
     * @return
     */
    List<ConfigModel> getConfigModelAll();

    /**
     * 校验文件格式
     *
     * @param filename
     */
    void checkFileSuffix(String filename);

    /**
     * 更新配置
     *
     * @param file
     */
    void refreshConfig(File file);

    /**
     * 获取水印信息
     *
     * @param systemConfig
     * @return
     */
    String getWatermark(SystemConfig systemConfig);
}