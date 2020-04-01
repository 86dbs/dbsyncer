package org.dbsyncer.biz.checker;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/7 23:17
 */
public interface Checker {

    /**
     * 修改配置
     *
     * @param params 修改参数
     * @return
     */
    String checkConfigModel(Map<String, String> params);

}