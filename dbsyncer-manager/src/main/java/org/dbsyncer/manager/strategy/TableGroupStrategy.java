package org.dbsyncer.manager.strategy;

import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.manager.template.GroupStrategy;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.TableGroup;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/12/2 22:53
 */
public class TableGroupStrategy implements GroupStrategy {

    @Override
    public String getGroupId(ConfigModel model) {
        if (model instanceof TableGroup) {
            TableGroup t = (TableGroup) model;
            String type = t.getType();
            String mappingId = t.getMappingId();
            // 格式：${type} + "_" + ${mappingId}
            return new StringBuilder(type).append("_").append(mappingId).toString();
        }
        throw new ManagerException(String.format("Not support config model \"%s\".", model));
    }

}