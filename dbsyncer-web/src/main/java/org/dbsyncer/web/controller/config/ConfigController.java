package org.dbsyncer.web.controller.config;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.CacheService;
import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.storage.impl.SnowflakeIdWorker;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
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
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

@Controller
@RequestMapping("/config")
public class ConfigController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private CacheService cacheService;

    @Resource
    private LogService logService;

    @Resource
    private AppConfig appConfig;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @RequestMapping("")
    public String index(ModelMap model) {
        model.put("config", systemConfigService.getConfigModelAll());
        model.put("fileSize", JsonUtil.objToJson(cacheService.getAll()).getBytes(Charset.defaultCharset()).length);
        return "config/config";
    }

    @PostMapping(value = "/getAll")
    @ResponseBody
    public RestResult getAll() {
        try {
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/upload")
    @ResponseBody
    public RestResult upload(MultipartFile[] files) {
        try {
            if (files != null && files.length > 0) {
                for (int i = 0; i < files.length; i++) {
                    if (files[i] == null) {
                        continue;
                    }
                    String filename = files[i].getOriginalFilename();
                    systemConfigService.checkFileSuffix(filename);
                    String tmpdir = System.getProperty("java.io.tmpdir");
                    File dest = new File(tmpdir + filename);
                    FileUtils.deleteQuietly(dest);
                    FileUtils.copyInputStreamToFile(files[i].getInputStream(), dest);
                    systemConfigService.refreshConfig(dest);
                    String msg = String.format("导入配置文件%s", filename);
                    logger.info(msg);
                    logService.log(LogType.CacheLog.IMPORT, msg);
                }
            }
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/download")
    public void download(HttpServletResponse response) {
        String fileName = String.format("%s-%s-%s.json", appConfig.getName(), appConfig.getVersion(), snowflakeIdWorker.nextId());
        response.setHeader("content-type", "application/octet-stream");
        response.setHeader("Content-Disposition", String.format("attachment; filename=%s", fileName));
        response.setContentType("application/octet-stream");
        OutputStream outputStream = null;
        try {
            outputStream = response.getOutputStream();
            String cache = JsonUtil.objToJson(cacheService.getAll());
            byte[] bytes = cache.getBytes(Charset.defaultCharset());
            int length = bytes.length;
            String msg = String.format("导出配置文件%s，大小%dKB", fileName, (length / 1024));
            logger.info(msg);
            logService.log(LogType.CacheLog.EXPORT, msg);
            outputStream.write(bytes, 0, length);
            outputStream.flush();
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            IOUtils.closeQuietly(outputStream);
        }
    }

}