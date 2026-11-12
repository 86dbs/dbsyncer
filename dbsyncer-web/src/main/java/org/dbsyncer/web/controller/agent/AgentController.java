/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.agent;

import org.dbsyncer.biz.AgentConfigService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.spi.AgentService;
import org.dbsyncer.web.controller.BaseController;
import org.dbsyncer.web.log.OperLog;
import org.dbsyncer.web.log.OperLogType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

/**
 * Agent 管理。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-12
 */
@Controller
@RequestMapping("/agent")
public class AgentController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private AgentService agentService;

    @Resource
    private AgentConfigService agentConfigService;

    /**
     * 列表页
     */
    @GetMapping("/list")
    public String list(ModelMap model) {
        initEditionInfo(model);
        model.put("agentAvailable", agentService.isAvailable());
        return "agent/list";
    }

    /**
     * 新增页
     */
    @GetMapping("/page/add")
    public String pageAdd(ModelMap model) {
        initEditionInfo(model);
        return "agent/add";
    }

    /**
     * 编辑页
     */
    @GetMapping("/page/edit")
    public String pageEdit(ModelMap model, @RequestParam("id") String id) {
        initEditionInfo(model);
        model.put("agent", agentConfigService.get(id));
        return "agent/edit";
    }

    /**
     * 分页查询（当前返回全量列表，前端分页）
     */
    @PostMapping("/query")
    @ResponseBody
    public RestResult query() {
        try {
            requirePermission("agent:view");
            List<Map<String, Object>> list = agentConfigService.list();
            Paging<Map<String, Object>> paging = new Paging<>(1, list == null ? 0 : list.size());
            paging.setData(list);
            paging.setTotal(list == null ? 0 : list.size());
            return RestResult.restSuccess(paging);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 新增
     */
    @OperLog(type = OperLogType.SYSTEM_INFO, message = "新增Agent")
    @PostMapping("/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            requirePermission("agent:edit");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(agentConfigService.add(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 编辑
     */
    @OperLog(type = OperLogType.SYSTEM_INFO, message = "修改Agent")
    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            requirePermission("agent:edit");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(agentConfigService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 删除
     */
    @OperLog(type = OperLogType.SYSTEM_INFO, message = "删除Agent")
    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam("id") String id) {
        try {
            requirePermission("agent:edit");
            agentConfigService.remove(id);
            return RestResult.restSuccess("删除成功");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 启动
     */
    @OperLog(type = OperLogType.SYSTEM_INFO, message = "启动Agent")
    @PostMapping("/start")
    @ResponseBody
    public RestResult start(@RequestParam("id") String id) {
        try {
            requirePermission("agent:edit");
            agentService.start(id);
            return RestResult.restSuccess("启动成功");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 停止
     */
    @OperLog(type = OperLogType.SYSTEM_INFO, message = "停止Agent")
    @PostMapping("/stop")
    @ResponseBody
    public RestResult stop(@RequestParam("id") String id) {
        try {
            requirePermission("agent:edit");
            agentService.stop(id);
            return RestResult.restSuccess("停止成功");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 生成密钥
     */
    @PostMapping("/generateSecret")
    @ResponseBody
    public RestResult generateSecret() {
        try {
            requirePermission("agent:edit");
            return RestResult.restSuccess(agentConfigService.generateSecret());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 详情
     */
    @PostMapping("/get")
    @ResponseBody
    public RestResult get(@RequestParam("id") String id) {
        try {
            requirePermission("agent:view");
            if (StringUtil.isBlank(id)) {
                return RestResult.restFail("ID不能为空");
            }
            return RestResult.restSuccess(agentConfigService.get(id));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }
}
