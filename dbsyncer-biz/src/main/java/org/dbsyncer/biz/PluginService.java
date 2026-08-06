/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.PluginVO;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/13 17:18
 */
public interface PluginService {

    /**
     * 获取已安装插件列表（轻量，不含驱动关联信息）。
     * 用于驱动/表映射编辑页下拉，避免扫描全部任务。
     *
     * @return 已安装插件
     */
    List<PluginVO> listPlugins();

    /**
     * 获取所有插件（含正在使用该插件的驱动名称）。
     * 仅插件管理页使用，会扫描全部驱动与表映射。
     *
     * @return 插件及关联驱动名
     */
    List<PluginVO> getPluginAll();

    /**
     * 获取插件上传路径
     *
     * @return
     */
    String getPluginPath();

    /**
     * 获取开发包路径
     *
     * @return
     */
    String getLibraryPath();

    /**
     * 加载插件
     */
    void loadPlugins();

    /**
     * 检查文件格式
     *
     * @param filename
     */
    void checkFileSuffix(String filename);
}
