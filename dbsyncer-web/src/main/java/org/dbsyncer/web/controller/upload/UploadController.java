package org.dbsyncer.web.controller.upload;

import org.apache.commons.io.FileUtils;
import org.dbsyncer.biz.PluginService;
import org.dbsyncer.biz.vo.RestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;

@Controller
@RequestMapping("/upload")
public class UploadController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @RequestMapping("")
    public String index(ModelMap model) {
        return "upload/upload";
    }

    @Autowired
    private PluginService pluginService;

    @RequestMapping(value = "/upload")
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

}