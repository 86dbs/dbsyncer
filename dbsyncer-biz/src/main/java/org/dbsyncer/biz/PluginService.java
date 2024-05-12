/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.PluginVo;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/13 17:18
 */
public interface PluginService {

    /**
     * 获取所有插件
     *
     * @return
     */
    List<PluginVo> getPluginAll();

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