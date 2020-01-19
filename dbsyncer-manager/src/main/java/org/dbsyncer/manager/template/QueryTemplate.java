package org.dbsyncer.manager.template;

import org.dbsyncer.parser.model.ConfigModel;

/**
 * 查询模板
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
public interface QueryTemplate<T> extends BaseTemplate {

    ConfigModel getConfigModel();

}