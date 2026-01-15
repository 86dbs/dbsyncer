/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.vo.MappingJsonVo;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.AppConfigService;
import org.dbsyncer.biz.ProjectGroupService;
import org.dbsyncer.biz.vo.ProjectGroupVo;
import org.dbsyncer.biz.vo.RestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * @author cdeluser
 */
@Controller
@RequestMapping("/index")
public class IndexController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ProjectGroupService projectGroupService;

    @Resource
    private AppConfigService appConfigService;

    @GetMapping("")
    public String index(ModelMap model, String projectGroupId) {
        try {
            ProjectGroupVo projectGroup = new ProjectGroupVo();
            if ("".equals(projectGroupId)) {
                projectGroup = projectGroupService.getProjectGroupUnUsed();
            }else {
                projectGroup = projectGroupService.getProjectGroup(projectGroupId);
            }
            model.put("connectorSize", projectGroup.getConnectorSize());
            model.put("connectors", projectGroup.getConnectors());
            model.put("mappings", projectGroup.getMappings());
            model.put("projectGroupId", projectGroupId);
            model.put("projectGroups", projectGroupService.getProjectGroupAll());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return "index/index.html";
    }

    @ResponseBody
    @GetMapping("/mappingdata")
    public RestResult mappingdata(ModelMap model, String projectGroupId) {
        try {
            ProjectGroupVo projectGroup = new ProjectGroupVo();
            if ("".equals(projectGroupId)) {
                projectGroup = projectGroupService.getProjectGroupUnUsed();
                projectGroupId = "";
            }else {
                projectGroup = projectGroupService.getProjectGroup(projectGroupId);
            }
            List<MappingVo> mappings = projectGroup.getMappings();
//            model.put("connectorSize", projectGroup.getConnectorSize());
//            model.put("connectors", projectGroup.getConnectors());
//            model.put("mappings",mappings);
//            model.put("projectGroupId", projectGroupId);
//            model.put("projectGroups", projectGroupService.getProjectGroupAll());
            List<MappingJsonVo> resultMapping = new ArrayList<>();
            for (MappingVo mapping : mappings){
                MappingJsonVo mappingJsonVo =new MappingJsonVo();
                mappingJsonVo.setId(mapping.getId());
                mappingJsonVo.setMeta(mapping.getMeta());
                mappingJsonVo.setModel(mapping.getModel());
                resultMapping.add(mappingJsonVo);
            }
            return RestResult.restSuccess(resultMapping);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return RestResult.restFail(e.getMessage(),-10);
        }
    }


    /**
     * 根据驱动名称进行模糊搜索
     * @param keyword 搜索关键词
     * @param projectGroupId 项目组ID
     * @param state 任务状态
     * @param sourceConnectorType 数据源类型
     * @param targetConnectorType 目标源类型
     * @return 匹配的驱动列表
     */
    @GetMapping("/searchMapping")
    @ResponseBody
    public RestResult searchMapping(String keyword, String projectGroupId, String state, String sourceConnectorType, String targetConnectorType) {
        try {
            ProjectGroupVo projectGroup = new ProjectGroupVo();
            if ("".equals(projectGroupId)) {
                projectGroup = projectGroupService.getProjectGroupUnUsed();
                projectGroupId = "";
            } else {
                projectGroup = projectGroupService.getProjectGroup(projectGroupId);
            }
            List<MappingVo> mappings = projectGroup.getMappings();
            List<MappingJsonVo> resultMapping = new ArrayList<>();

            for (MappingVo mapping : mappings) {
                boolean matchKeyword = keyword == null || keyword.trim().isEmpty() ||
                        (mapping.getName() != null && mapping.getName().toLowerCase().contains(keyword.toLowerCase()));
                
                boolean matchState = state == null || state.trim().isEmpty() ||
                        (mapping.getMeta() != null && String.valueOf(mapping.getMeta().getState()).equals(state));
                
                boolean matchSource = sourceConnectorType == null || sourceConnectorType.trim().isEmpty() ||
                        (mapping.getSourceConnector() != null && mapping.getSourceConnector().getConfig() != null && 
                         sourceConnectorType.equals(mapping.getSourceConnector().getConfig().getConnectorType()));
                
                boolean matchTarget = targetConnectorType == null || targetConnectorType.trim().isEmpty() ||
                        (mapping.getTargetConnector() != null && mapping.getTargetConnector().getConfig() != null && 
                         targetConnectorType.equals(mapping.getTargetConnector().getConfig().getConnectorType()));

                if (matchKeyword && matchState && matchSource && matchTarget) {
                    MappingJsonVo mappingJsonVo = new MappingJsonVo();
                    mappingJsonVo.setId(mapping.getId());
                    mappingJsonVo.setMeta(mapping.getMeta());
                    mappingJsonVo.setModel(mapping.getModel());
                    resultMapping.add(mappingJsonVo);
                }
            }

            return RestResult.restSuccess(resultMapping);
        } catch (Exception e) {
            logger.error("搜索驱动失败: " + e.getMessage(), e);
            return RestResult.restFail("搜索驱动失败: " + e.getMessage());
        }
    }


    @GetMapping("/version.json")
    @ResponseBody
    public RestResult version() throws Exception {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return RestResult.restSuccess(appConfigService.getVersionInfo(authentication.getName()));
    }
}