package org.dbsyncer.web.controller.upload;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.dbsyncer.biz.PluginService;
import org.dbsyncer.biz.vo.RestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.*;

@Controller
@RequestMapping("/upload")
public class UploadController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 版本号
     */
    @Value(value = "${info.app.version}")
    private String version;

    @RequestMapping("")
    public String index(ModelMap model) {
        model.put("plugins", pluginService.getPluginAll());
        model.put("version", version);
        return "upload/upload";
    }

    @Autowired
    private PluginService pluginService;

    @PostMapping(value = "/upload")
    @ResponseBody
    public RestResult upload(MultipartFile[] files) {
        try {
            if (files != null && files.length > 0) {
                MultipartFile file = null;
                String filePath = pluginService.getPluginPath();
                FileUtils.forceMkdir(new File(filePath));
                for (int i = 0; i < files.length; i++) {
                    file = files[i];
                    if (file != null) {
                        String filename = file.getOriginalFilename();
                        pluginService.checkFileSuffix(filename);
                        File dest = new File(filePath + filename);
                        FileUtils.deleteQuietly(dest);
                        FileUtils.copyInputStreamToFile(file.getInputStream(), dest);
                    }
                }
                pluginService.loadPlugins();
            }
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/download")
    public void download(HttpServletResponse response) {
        String fileName = String.format("dbsyncer-common-%s.jar", version);
        response.setHeader("content-type", "application/octet-stream");
        response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
        response.setContentType("application/octet-stream");
        BufferedInputStream bis = null;
        OutputStream outputStream = null;
        try {
            outputStream = response.getOutputStream();
            String filePath = pluginService.getLibraryPath();
            bis = new BufferedInputStream(new FileInputStream(new File(filePath + fileName)));
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

}