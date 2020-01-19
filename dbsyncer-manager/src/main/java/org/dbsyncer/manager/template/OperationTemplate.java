package org.dbsyncer.manager.template;

import org.dbsyncer.manager.template.impl.ConfigOperationTemplate;
import org.dbsyncer.parser.model.ConfigModel;

/**
 * 操作模板
 *
 * @author AE86
 * @version 1.0.0
 * @date 2019/9/16 23:59
 */
public interface OperationTemplate extends BaseTemplate {

    ConfigModel parseConfigModel();

    void handleEvent(ConfigOperationTemplate.Call call);

}