package org.dbsyncer.manager;

import org.dbsyncer.parser.model.ConfigModel;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/12/2 22:52
 */
public interface GroupStrategy {

    String getGroupId(ConfigModel model);
    
}