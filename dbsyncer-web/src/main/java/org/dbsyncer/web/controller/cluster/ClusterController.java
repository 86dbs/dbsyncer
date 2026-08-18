/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.cluster;

import org.dbsyncer.biz.ClusterManagerService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.web.controller.BaseController;
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
import java.util.HashMap;
import java.util.Map;

/**
 * 集群管理。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
@Controller
@RequestMapping("/cluster")
public class ClusterController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ClusterManagerService clusterManagerService;

    /**
     * 集群列表页。
     */
    @GetMapping("/list")
    public String list(ModelMap model) {
        model.put("clusterEnabled", clusterManagerService.isClusterEnabled());
        model.put("current", clusterManagerService.current());
        return "cluster/list";
    }

    /**
     * 心跳探测（免登录，供节点互探）。
     */
    @GetMapping("/ping")
    @ResponseBody
    public RestResult ping() {
        return RestResult.restSuccess("ok");
    }

    /**
     * 节点分页。
     */
    @PostMapping("/query")
    @ResponseBody
    public RestResult query(HttpServletRequest request) {
        try {
            return RestResult.restSuccess(clusterManagerService.query(getParams(request)));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 切换 Leader。
     */
    @PostMapping("/transfer")
    @ResponseBody
    public RestResult transfer(@RequestParam("id") String id) {
        try {
            clusterManagerService.transferLeadership(id);
            return RestResult.restSuccess("已发起切换");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 移除节点。
     */
    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam("id") String id) {
        try {
            clusterManagerService.removeNode(id);
            return RestResult.restSuccess("已移除");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 运维状态 JSON。
     */
    @GetMapping("/status.json")
    @ResponseBody
    public RestResult status() {
        try {
            Map<String, Object> data = new HashMap<>();
            data.put("clusterEnabled", clusterManagerService.isClusterEnabled());
            data.put("current", clusterManagerService.current());
            data.put("nodes", clusterManagerService.query(new HashMap<>()).getData());
            return RestResult.restSuccess(data);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }
}
