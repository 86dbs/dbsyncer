package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.ConfigVo;

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
    ConfigVo getConfig();

    /**
     * 获取密码
     *
     * @return
     */
    String getPassword();

    /**
     * 获取所有配置
     *
     * @return
     */
    List<ConfigVo> queryConfig();

}