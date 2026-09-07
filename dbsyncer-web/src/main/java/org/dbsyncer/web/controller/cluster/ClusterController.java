/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.cluster;

import org.dbsyncer.biz.ClusterManagerService;
import org.dbsyncer.biz.vo.EditionInfoVO;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.sdk.spi.LicenseService;
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

    @Resource
    private LicenseService licenseService;

    @Resource
    private LocalNodeMetricProvider localNodeMetricProvider;

    @Resource
    private ClusterNodeMetricAggregator clusterNodeMetricAggregator;

    /**
     * 集群列表页。
     */
    @GetMapping("/list")
    public String list(ModelMap model) {
        EditionInfoVO editionInfo = new EditionInfoVO();
        editionInfo.setEdition(licenseService.getEditionEnum().getCode());
        editionInfo.setEditionName(licenseService.getEditionEnum().getMessage());
        model.put("editionInfo", editionInfo);
        model.put("clusterEnabled", clusterManagerService.isClusterEnabled());
        return "cluster/list";
    }

//    /**
//     * 内部拉起本机执行器。
//     */
//    @PostMapping("/internal/execute")
//    @ResponseBody
//    public RestResult executeLocal(@RequestParam("taskId") String taskId) {
//        try {
//            if (clusterManagerService.isClusterEnabled()) {
//                return RestResult.restFail("单机不支持内部执行接口");
//            }
//            boolean ok = clusterService.executeLocal(taskId);
//            return ok ? RestResult.restSuccess("ok") : RestResult.restFail("本机不是该任务的调度节点");
//        } catch (Exception e) {
//            logger.error(e.getLocalizedMessage(), e);
//            return RestResult.restFail(e.getMessage());
//        }
//    }

//    /**
//     * 内部停止本机执行器。
//     */
//    @PostMapping("/internal/stop")
//    @ResponseBody
//    public RestResult stopLocal(@RequestParam("taskId") String taskId) {
//        try {
//            if (clusterService.isStandalone()) {
//                return RestResult.restFail("单机不支持内部停止接口");
//            }
//            clusterService.stopExecuteLocal(taskId);
//            return RestResult.restSuccess("ok");
//        } catch (Exception e) {
//            logger.error(e.getLocalizedMessage(), e);
//            return RestResult.restFail(e.getMessage());
//        }
//    }
//
//    /**
//     * 内部拉起本机分片。
//     */
//    @PostMapping("/internal/shard/execute")
//    @ResponseBody
//    public RestResult executeShardLocal(@RequestParam("planId") String planId) {
//        try {
//            if (clusterService.isStandalone()) {
//                return RestResult.restFail("单机不支持内部分片执行接口");
//            }
//            boolean ok = clusterService.executeShardLocal(planId);
//            return ok ? RestResult.restSuccess("ok") : RestResult.restFail("本机不是该分片的执行节点");
//        } catch (Exception e) {
//            logger.error(e.getLocalizedMessage(), e);
//            return RestResult.restFail(e.getMessage());
//        }
//    }
//
//    /**
//     * 内部停止本机分片。
//     */
//    @PostMapping("/internal/shard/stop")
//    @ResponseBody
//    public RestResult stopShardLocal(@RequestParam("planId") String planId) {
//        try {
//            if (clusterService.isStandalone()) {
//                return RestResult.restFail("单机不支持内部分片停止接口");
//            }
//            clusterService.stopShardLocal(planId);
//            return RestResult.restSuccess("ok");
//        } catch (Exception e) {
//            logger.error(e.getLocalizedMessage(), e);
//            return RestResult.restFail(e.getMessage());
//        }
//    }

    /**
     * 心跳探测（免登录，供节点互探）。
     */
    @GetMapping("/ping")
    @ResponseBody
    public RestResult ping() {
        return RestResult.restSuccess("ok");
    }

    /**
     * 本机运行指标（免登录，供集群内节点互拉）。
     */
    @GetMapping("/metrics")
    @ResponseBody
    public RestResult metrics() {
        try {
            return RestResult.restSuccess(localNodeMetricProvider.snapshot());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 聚合各节点运行指标（本机直采 + 远端 HTTP）。
     */
    @GetMapping("/nodes/metrics")
    @ResponseBody
    public RestResult nodesMetrics() {
        try {
            return RestResult.restSuccess(clusterNodeMetricAggregator.collectAll());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
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
     * 修改节点名称
     */
    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(@RequestParam("id") String id, @RequestParam("name") String name) {
        try {
            clusterManagerService.updateNodeName(id, name);
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
            clusterManagerService.removeNode(id);
            return RestResult.restSuccess("删除节点成功");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

}
