package org.dbsyncer.biz.checker;

import org.dbsyncer.parser.model.ConfigModel;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 23:17
 */
public interface Checker {

    /**
     * 新增配置
     *
     * @param params
     * @return
     */
    ConfigModel checkAddConfigModel(Map<String, String> params);

    /**
     * 修改配置
     *
     * @param params
     * @return
     */
    ConfigModel checkEditConfigModel(Map<String, String> params);
}
