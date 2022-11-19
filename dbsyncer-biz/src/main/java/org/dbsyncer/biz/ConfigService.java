package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.SystemConfigVo;
import org.dbsyncer.parser.model.ConfigModel;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/5/30 23:14
 */
public interface ConfigService {

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
    SystemConfigVo getConfig();

    /**
     * 获取所有配置
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
}