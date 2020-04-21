package org.dbsyncer.manager.template.impl;

import org.dbsyncer.manager.ManagerException;
import org.dbsyncer.manager.template.GroupStrategy;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Meta;
import org.springframework.stereotype.Component;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/21 21:35
 */
@Component
public class MetaGroupStrategy implements GroupStrategy {

    @Override
    public String getGroupId(ConfigModel model) {
        if (model instanceof Meta) {
            Meta m = (Meta) model;
            String type = m.getType();
            String mappingId = m.getMappingId();
            // 格式：${type} + "_" + ${mappingId}
            return new StringBuilder(type).append("_").append(mappingId).toString();
        }
        throw new ManagerException(String.format("Not support config model \"%s\".", model));
    }

}