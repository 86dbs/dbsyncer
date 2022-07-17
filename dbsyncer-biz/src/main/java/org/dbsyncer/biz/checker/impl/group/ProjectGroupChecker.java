package org.dbsyncer.biz.checker.impl.group;

import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.ProjectGroup;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.dbsyncer.storage.constant.ConfigConstant.CONFIG_MODEL_ID;
import static org.dbsyncer.storage.constant.ConfigConstant.PROJECT_GROUP;

/**
 * @author xinpeng.Fu
 * @version 1.0.0
 * @date 2022/6/9 17:09
 **/
@Component
public class ProjectGroupChecker extends AbstractChecker {

    @Autowired
    private Manager manager;

    /**
     * 新增配置
     *
     * @param params
     * @return
     */
    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        String mappingIds = params.get("mappingIds");
        String connectorIds = params.get("connectorIds");
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        ProjectGroup projectGroup = new ProjectGroup();
        projectGroup.setMappingIds(StringUtil.isBlank(mappingIds) ? Collections.emptyList() : Arrays.asList(mappingIds.split(",")));
        projectGroup.setConnectorIds(StringUtil.isBlank(connectorIds) ? Collections.emptyList() : Arrays.asList(connectorIds.split(",")));
        projectGroup.setType(PROJECT_GROUP);
        projectGroup.setName(name);

        // 修改基本配置
        this.modifyConfigModel(projectGroup, params);

        return projectGroup;
    }

    /**
     * 修改配置
     *
     * @param params
     * @return
     */
    @Override
    public ConfigModel checkEditConfigModel(Map<String, String> params) {
        String id = params.get(CONFIG_MODEL_ID);
        ProjectGroup projectGroup = manager.getProjectGroup(id);
        Assert.notNull(projectGroup, "Can not find project group.");

        // TODO

        // 修改基本配置
        this.modifyConfigModel(projectGroup, params);

        return projectGroup;
    }
}
