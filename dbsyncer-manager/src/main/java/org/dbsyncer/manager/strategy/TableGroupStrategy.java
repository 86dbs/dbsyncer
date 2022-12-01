package org.dbsyncer.manager.strategy;

import org.dbsyncer.manager.GroupStrategy;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.storage.constant.ConfigConstant;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/12/2 22:53
 */
public class TableGroupStrategy implements GroupStrategy<TableGroup> {

    @Override
    public String getGroupId(TableGroup model) {
        String mappingId = model.getMappingId();
        return new StringBuilder(ConfigConstant.TABLE_GROUP).append("_").append(mappingId).toString();
    }

}