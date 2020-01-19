package org.dbsyncer.manager.template;

import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.storage.constant.ConfigConstant;

/**
 * 预加载配置模板
 */
public interface PreLoadTemplate extends BaseTemplate {

    /**
     * 过滤类型
     *
     * @return
     * @see ConfigConstant
     */
    String filterType();

    /**
     * 解析json配置实现
     *
     * @param json JSON配置
     * @return
     */
    ConfigModel parseModel(String json);

}