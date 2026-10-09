/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.cluster;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.model.PluginFile;
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
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
            return RestResult.restSuccess(clusterService.receiveMessage(message));
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

    /**
     * 本机插件清单。须携带 {@code X-Cluster-Token}。
     */
    @GetMapping("/internal/plugin/manifest")
    @ResponseBody
    public RestResult pluginManifest() {
        try {
            return RestResult.restSuccess(clusterService.listPluginFiles());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 读取本机插件文件。须携带 {@code X-Cluster-Token}。
     */
    @GetMapping("/internal/plugin/file")
    public void pluginFile(HttpServletResponse response, @RequestParam("name") String name) {
        try {
            if (!PluginFile.isSafeName(name)) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            byte[] body = clusterService.readPluginFile(name);
            if (body == null) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            response.setContentType("application/octet-stream");
            response.setContentLength(body.length);
            OutputStream out = response.getOutputStream();
            out.write(body);
            out.flush();
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * 接收插件文件。文件名在请求头 {@link PluginFile#FILE_NAME_HEADER}。须携带 {@code X-Cluster-Token}。
     */
    @PostMapping("/internal/plugin/file")
    @ResponseBody
    public RestResult acceptPlugin(HttpServletRequest request) {
        try {
            String fileName = request.getHeader(PluginFile.FILE_NAME_HEADER);
            boolean relay = StringUtil.equals("1", request.getHeader(PluginFile.RELAY_HEADER));
            clusterService.acceptPluginFile(fileName, readPluginBody(request), relay);
            return RestResult.restSuccess(Boolean.TRUE);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    private byte[] readPluginBody(HttpServletRequest request) throws IOException {
        long length = request.getContentLengthLong();
        if (length > PluginFile.MAX_BYTES) {
            throw new BizException("插件文件超过128MB");
        }
        int initial = length > 0 && length <= Integer.MAX_VALUE ? (int) length : 8192;
        ByteArrayOutputStream out = new ByteArrayOutputStream(initial);
        InputStream in = request.getInputStream();
        byte[] buf = new byte[8192];
        long total = 0;
        int n;
        while ((n = in.read(buf)) != -1) {
            total += n;
            if (total > PluginFile.MAX_BYTES) {
                throw new BizException("插件文件超过128MB");
            }
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

}
