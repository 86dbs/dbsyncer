package org.dbsyncer.web.controller.plugin;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.PluginService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.sdk.model.PluginFile;
import org.dbsyncer.sdk.spi.ClusterService;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping("/plugin")
public class PluginController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private PluginService pluginService;

    @Resource
    private ClusterService clusterService;

    @Resource
    private AppConfig appConfig;

    @RequestMapping("")
    public String index(ModelMap model) {
        model.put("plugins", pluginService.listPlugins());
        model.put("version", appConfig.getVersion());
        return "plugin/list";
    }

    @PostMapping(value = "/upload")
    @ResponseBody
    public RestResult upload(MultipartFile[] files) {
        try {
            if (files != null && files.length > 0) {
                String filePath = pluginService.getPluginPath();
                FileUtils.forceMkdir(new File(filePath));
                List<String> saved = new ArrayList<String>();
                for (MultipartFile file : files) {
                    if (file == null) {
                        continue;
                    }
                    String filename = fileName(file.getOriginalFilename());
                    pluginService.checkFileSuffix(filename);
                    if (!PluginFile.isSafeName(filename)) {
                        throw new BizException("插件文件名不合法，仅支持字母、数字、点、下划线和短横线");
                    }
                    File dest = new File(filePath, filename);
                    FileUtils.deleteQuietly(dest);
                    FileUtils.copyInputStreamToFile(file.getInputStream(), dest);
                    saved.add(filename);
                }
                pluginService.loadPlugins();
                for (String filename : saved) {
                    clusterService.publishPlugin(filename);
                }
            }
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/download")
    public void download(HttpServletResponse response, String name) {
        String fileName = String.format("dbsyncer-%s-%s.jar", name, appConfig.getVersion());
        File file = new File(pluginService.getLibraryPath() + fileName);
        if (!file.exists()) {
            write(response, RestResult.restFail("Could not find file", 404));
            return;
        }

        response.setHeader("content-type", "application/octet-stream");
        response.setHeader("Content-Disposition", String.format("attachment; filename=%s", fileName));
        response.setContentType("application/octet-stream");
        BufferedInputStream bis = null;
        OutputStream outputStream = null;
        try {
            outputStream = response.getOutputStream();
            bis = new BufferedInputStream(new FileInputStream(file));
            byte[] buff = new byte[2048];
            int read = bis.read(buff);
            while (read != -1) {
                outputStream.write(buff, 0, buff.length);
                outputStream.flush();
                read = bis.read(buff);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            IOUtils.closeQuietly(bis);
            IOUtils.closeQuietly(outputStream);
        }
    }

    private String fileName(String original) {
        if (original == null) {
            return null;
        }
        return new File(original).getName();
    }

    /**
     * 响应
     *
     * @param response
     * @param result
     */
    private void write(HttpServletResponse response, RestResult result) {
        PrintWriter out = null;
        try {
            response.setContentType("application/json;charset=utf-8");
            response.setStatus(result.getStatus());
            out = response.getWriter();
            out.write(JsonUtil.objToJson(result));
            out.flush();
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            if (null != out) {
                out.close();
            }
        }
    }
}
