package org.dbsyncer.manager.strategy;

import org.dbsyncer.manager.GroupStrategy;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.storage.constant.ConfigConstant;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/12/2 22:53
 */
public class PreloadTableGroupStrategy implements GroupStrategy<Mapping> {

    @Override
    public String getGroupId(Mapping model) {
        return new StringBuilder(ConfigConstant.TABLE_GROUP).append("_").append(model.getId()).toString();
    }

}