package org.dbsyncer.web.controller.index;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.MappingService;
import org.dbsyncer.biz.TargetTableNotExistsException;
import org.dbsyncer.biz.TableGroupService;
import org.dbsyncer.biz.vo.MappingJsonVo;
import org.dbsyncer.biz.vo.MappingVo;
import org.dbsyncer.biz.vo.ProjectGroupVo;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/mapping")
public class MappingController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ConnectorService connectorService;

    @Resource
    private MappingService mappingService;

    @Resource
    private TableGroupService tableGroupService;

    @GetMapping("/pageAdd")
    public String page(ModelMap model) {
        model.put("connectors", connectorService.getConnectorAll());
        return "mapping/add";
    }

    @GetMapping("/page/{page}")
    public String page(ModelMap model, @PathVariable("page") String page, @RequestParam(value = "id") String id, Integer classOn, Integer exclude) throws Exception {
        model.put("mapping", mappingService.getMapping(id, exclude));
        model.put("classOn", classOn);
        model.put("tableGroups", tableGroupService.getTableGroupAll(id));
        initConfig(model);
        return "mapping/" + page;
    }

    @GetMapping("/get")
    @ResponseBody
    public RestResult get(@RequestParam(value = "id") String id) {
        try {
            MappingVo mapping = mappingService.getMapping(id);
            //因为meta属性被@JsonIgnore了  使用新的数据类型
            MappingJsonVo mappingJsonVo = new MappingJsonVo();
            mappingJsonVo.setId(mapping.getId());
            mappingJsonVo.setModel(mapping.getModel());
            mappingJsonVo.setMeta(mapping.getMeta());
            return RestResult.restSuccess(mappingJsonVo);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/getTableGroups")
    @ResponseBody
    public RestResult getTableGroups(@RequestParam(value = "id") String id) {
        try {
            List<TableGroup> tableGroups = tableGroupService.getTableGroupAll(id);
            return RestResult.restSuccess(tableGroups);
        } catch (Exception e) {
            logger.error("获取表映射关系失败", e);
            return RestResult.restFail("获取表映射关系失败: " + e.getMessage());
        }
    }

    /**
     * 获取与特定数据源关联的驱动列表
     * @param connectorId
     * @return
     */

    @GetMapping("/getRelatedMappings")
    @ResponseBody
    public RestResult getRelatedMappings(@RequestParam(value = "connectorId") String connectorId) {
        try {
            List<MappingVo> allMappings = mappingService.getMappingAll();
            // 筛选与指定数据源相关的映射（作为源或目标）
            List<MappingVo> relatedMappings = allMappings.stream()
                    .filter(mapping -> connectorId.equals(mapping.getSourceConnectorId()) ||
                            connectorId.equals(mapping.getTargetConnectorId()))
                    .collect(java.util.stream.Collectors.toList());
            return RestResult.restSuccess(relatedMappings);
        } catch (Exception e) {
            logger.error("获取关联映射列表失败", e);
            return RestResult.restFail("获取关联映射列表失败: " + e.getMessage());
        }
    }

    @PostMapping("/copy")
    @ResponseBody
    public RestResult add(@RequestParam("id") String id) {
        try {
            return RestResult.restSuccess(mappingService.copy(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(mappingService.add(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(mappingService.edit(params));
        } catch (TargetTableNotExistsException e) {
            // 目标表不存在异常，返回特殊错误码和详细信息
            logger.info("目标表不存在，等待用户确认: {}", e.getMissingTables());
            return e.toRestResult();
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.remove(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/start")
    @ResponseBody
    public RestResult start(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.start(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/stop")
    @ResponseBody
    public RestResult stop(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.stop(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/reset")
    @ResponseBody
    public RestResult reset(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.reset(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/refreshTables")
    @ResponseBody
    public RestResult refreshTables(@RequestParam(value = "id") String id) {
        try {
            return RestResult.restSuccess(mappingService.refreshMappingTables(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

}