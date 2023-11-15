/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.strategy.impl;

import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.strategy.GroupStrategy;
import org.dbsyncer.storage.constant.ConfigConstant;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/12/2 22:53
 */
public final class PreloadTableGroupStrategy implements GroupStrategy<Mapping> {

    @Override
    public String getGroupId(Mapping model) {
        return new StringBuilder(ConfigConstant.TABLE_GROUP).append("_").append(model.getId()).toString();
    }

}