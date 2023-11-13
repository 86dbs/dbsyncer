/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.strategy.impl;

import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.strategy.GroupStrategy;

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