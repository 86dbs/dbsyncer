package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.ProjectGroupService;
import org.dbsyncer.biz.checker.Checker;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.Manager;
import org.dbsyncer.parser.logger.LogType;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.ProjectGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 项目组
 *
 * @author xinpeng.Fu
 * @version 1.0.0
 * @date 2022/6/9 17:09
 **/
@Service
public class ProjectGroupServiceImpl extends BaseServiceImpl implements ProjectGroupService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

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
        Assert.notNull(projectGroup, "该项目组已被删除");
        manager.removeProjectGroup(id);
        return "删除项目组成功!";
    }

    @Override
    public ProjectGroup getProjectGroup(String id) {
        return StringUtil.isNotBlank(id) ? manager.getProjectGroup(id) : null;
    }

    @Override
    public List<ProjectGroup> getProjectGroupAll() {
        List<ProjectGroup> list = manager.getProjectGroupAll();
        return list;
    }

    @Override
    public ProjectGroup getProjectGroupDetail(String projectGroupId) {
        ProjectGroup projectGroup = this.getProjectGroup(projectGroupId);
        if(null == projectGroup){
            return null;
        }
        // 过滤出已经选择的
        List<String> connectorIds = projectGroup.getConnectorIds();
        if(CollectionUtils.isEmpty(connectorIds)){
            projectGroup.setConnectors(Collections.emptyList());
        }else{
            Set<String> connectorIdSet = new HashSet<>(connectorIds);
            List<Connector> connectors = connectorService.getConnectorAll();
            projectGroup.setConnectors(connectors.stream()
                    .filter((connector -> connectorIdSet.contains(connector.getId())))
                    .collect(Collectors.toList())
            );
        }
        List<String> mappingIds = projectGroup.getMappingIds();
        if(CollectionUtils.isEmpty(mappingIds)){
            projectGroup.setMappings(Collections.emptyList());
        }else{
            Set<String> mappingIdSet = new HashSet<>(mappingIds);
            List<MappingVo> mappings = mappingService.getMappingAll();
            projectGroup.setMappings(mappings.stream()
                    .filter((mapping -> mappingIdSet.contains(mapping.getId())))
                    .collect(Collectors.toList())
            );
        }
        return projectGroup;
    }
}
