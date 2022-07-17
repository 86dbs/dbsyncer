package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.ProjectGroupService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.ProjectGroupVo;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.ProjectGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 分组
 *
 * @author xinpeng.Fu
 * @version 1.0.0
 * @date 2022/6/9 17:09
 **/
@Service
public class ProjectGroupServiceImpl extends BaseServiceImpl implements ProjectGroupService {

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private MappingService mappingService;

    @Autowired
    private Manager manager;

    @Autowired
    private Checker projectGroupChecker;

    @Override
    public String add(Map<String, String> params) {
        ConfigModel model = projectGroupChecker.checkAddConfigModel(params);
        log(LogType.ConnectorLog.INSERT, model);

        return manager.addProjectGroup(model);
    }

    @Override
    public String edit(Map<String, String> params) {
        ConfigModel model = projectGroupChecker.checkEditConfigModel(params);
        log(LogType.ConnectorLog.UPDATE, model);

        return manager.editProjectGroup(model);
    }

    @Override
    public String remove(String id) {
        ProjectGroup projectGroup = manager.getProjectGroup(id);
        log(LogType.ConnectorLog.DELETE, projectGroup);
        Assert.notNull(projectGroup, "该分组已被删除");
        manager.removeProjectGroup(id);
        return "删除分组成功!";
    }

    @Override
    public ProjectGroupVo getProjectGroup(String id) {
        ProjectGroupVo vo = new ProjectGroupVo();
        if (StringUtil.isBlank(id)) {
            vo.setConnectors(connectorService.getConnectorAll());
            vo.setMappings(mappingService.getMappingAll());
            return vo;
        }

        ProjectGroup projectGroup = manager.getProjectGroup(id);
        Assert.notNull(projectGroup, "该分组已被删除");

        // 过滤连接器
        List<String> connectorIds = projectGroup.getConnectorIds();
        if (!CollectionUtils.isEmpty(connectorIds)) {
            Set<String> connectorIdSet = new HashSet<>(connectorIds);
            List<Connector> connectors = connectorService.getConnectorAll();
            if (!CollectionUtils.isEmpty(connectors)) {
                vo.setConnectors(connectors.stream()
                        .filter((connector -> connectorIdSet.contains(connector.getId())))
                        .collect(Collectors.toList())
                );
            }
        }

        // 过滤驱动
        List<String> mappingIds = projectGroup.getMappingIds();
        if (!CollectionUtils.isEmpty(mappingIds)) {
            Set<String> mappingIdSet = new HashSet<>(mappingIds);
            List<MappingVo> mappings = mappingService.getMappingAll();
            if (!CollectionUtils.isEmpty(mappings)) {
                vo.setMappings(mappings.stream()
                        .filter((mapping -> mappingIdSet.contains(mapping.getId())))
                        .collect(Collectors.toList())
                );
            }
        }
        return vo;
    }

    @Override
    public List<ProjectGroup> getProjectGroupAll() {
        List<ProjectGroup> list = manager.getProjectGroupAll();
        return list;
    }

}
