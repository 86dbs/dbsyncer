/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.group;

import org.dbsyncer.biz.checker.AbstractChecker;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.ProfileComponent;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.ProjectGroup;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * @author xinpeng.Fu
 * @version 1.0.0
 * @date 2022/6/9 17:09
 **/
@Component
public class ProjectGroupChecker extends AbstractChecker {

    @Resource
    private ProfileComponent profileComponent;

    /**
     * 新增配置
     *
     * @param params
     * @return
     */
    @Override
    public ConfigModel checkAddConfigModel(Map<String, String> params) {
        String name = params.get(ConfigConstant.CONFIG_MODEL_NAME);
        ProjectGroup projectGroup = new ProjectGroup();
        projectGroup.setName(name);

        modifyProjectGroup(projectGroup, params);

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
        String id = params.get(ConfigConstant.CONFIG_MODEL_ID);
        ProjectGroup projectGroup = profileComponent.getProjectGroup(id);
        Assert.notNull(projectGroup, "Can not find project group.");

        modifyProjectGroup(projectGroup, params);

        // 修改基本配置
        this.modifyConfigModel(projectGroup, params);
        return projectGroup;
    }

    private void modifyProjectGroup(ProjectGroup projectGroup, Map<String, String> params) {
        String[] connectorIds = StringUtil.split(params.get("connectorIds"), StringUtil.VERTICAL_LINE);
        String[] mappingIds = StringUtil.split(params.get("mappingIds"), StringUtil.VERTICAL_LINE);
        boolean exist = (connectorIds != null && connectorIds.length > 0) | (mappingIds != null && mappingIds.length > 0);
        Assert.isTrue(exist, "请选择连接或驱动.");

        projectGroup.setConnectorIds(CollectionUtils.isEmpty(connectorIds) ? Collections.EMPTY_LIST : Arrays.asList(connectorIds));
        projectGroup.setMappingIds(CollectionUtils.isEmpty(mappingIds) ? Collections.EMPTY_LIST : Arrays.asList(mappingIds));
    }

}