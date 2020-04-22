package org.dbsyncer.web.controller.upload;

import org.dbsyncer.biz.vo.RestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/upload")
public class UploadController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @RequestMapping("")
    public String index(ModelMap model) {
        return "upload/upload";
    }

    //@PreAuthorize("hasAuthority('ROLE_')")
    @RequestMapping(value = "/upload")
    @ResponseBody
    public RestResult upload(HttpServletRequest request, @RequestParam("file") MultipartFile[] files) {
        try {
            if (files != null) {
                int length = files.length;
                MultipartFile file = null;
                for (int i = 0; i < length; i++) {
                    file = files[i];
                    String fileName = file.getOriginalFilename();
                    System.out.println(fileName);
                }
            }
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

}