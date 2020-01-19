package org.dbsyncer.manager.template.impl;

import org.dbsyncer.manager.template.GroupStrategy;
import org.dbsyncer.parser.model.ConfigModel;
import org.springframework.stereotype.Component;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/12/2 22:53
 */
@Component
public class DefaultGroupStrategy implements GroupStrategy {

    @Override
    public String getGroupId(ConfigModel model) {
        return model.getType();
    }

}