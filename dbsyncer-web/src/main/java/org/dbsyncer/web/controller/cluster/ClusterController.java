/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.cluster;

import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.sdk.spi.ClusterService;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

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
    private ClusterService clusterService;

    @Resource
    private LocalNodeMetricProvider localNodeMetricProvider;

    @Resource
    private ClusterNodeMetricAggregator clusterNodeMetricAggregator;

    /**
     * 集群列表页
     */
    @GetMapping("/list")
    public String list(ModelMap model) {
        initEditionInfo(model);
        model.put("clusterEnabled", !clusterService.isStandalone());
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
     * 节点分页
     */
    @PostMapping("/query")
    @ResponseBody
    public RestResult query(HttpServletRequest request) {
        try {
            return RestResult.restSuccess(clusterService.query(getParams(request)));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 修改节点名称
     */
    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(@RequestParam("id") String id, @RequestParam("name") String name) {
        try {
            clusterService.updateNodeName(id, name);
            return RestResult.restSuccess("修改节点名称成功");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 删除离线节点
     */
    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(@RequestParam("id") String id) {
        try {
            clusterService.removeNode(id);
            return RestResult.restSuccess("删除节点成功");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 聚合各节点运行指标（本机直采 + 远端 HTTP）。
     */
    @GetMapping("/metrics")
    @ResponseBody
    public RestResult nodesMetrics() {
        try {
            return RestResult.restSuccess(clusterNodeMetricAggregator.collectAll());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/forceExpireGracePeriod")
    @ResponseBody
    public RestResult forceExpireGracePeriod() {
        try {
            return RestResult.restSuccess(clusterService.forceExpireGracePeriod());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 内部统一消息入口（按 type/event 路由）。须携带 {@code X-Cluster-Token}。
     */
    @PostMapping("/internal/message")
    @ResponseBody
    public RestResult message(@RequestBody String message) {
        try {
            return RestResult.restSuccess(clusterService.handleMessage(message));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/internal/metrics")
    @ResponseBody
    public RestResult metrics() {
        try {
            return RestResult.restSuccess(localNodeMetricProvider.snapshot());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

}
