package org.dbsyncer.manager.strategy;

import org.dbsyncer.manager.template.GroupStrategy;
import org.dbsyncer.parser.model.ConfigModel;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/12/2 22:53
 */
public class DefaultGroupStrategy implements GroupStrategy {

    @Override
    public String getGroupId(ConfigModel model) {
        return model.getType();
    }

}