/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.web.controller.license;

import org.apache.commons.io.FileUtils;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.sdk.spi.LicenseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.File;

@Controller
@RequestMapping("/license")
public class LicenseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private LogService logService;

    @Resource
    private LicenseService licenseService;

    @RequestMapping("")
    public String index(ModelMap model) {
        model.put("key", licenseService.getKey());
        model.put("productInfo", licenseService.getProductInfo());
        return "license/license";
    }

    @PostMapping(value = "/upload")
    @ResponseBody
    public RestResult upload(MultipartFile[] files) {
        try {
            if (files != null && files[0] != null) {
                String filename = files[0].getOriginalFilename();
                File dest = new File(licenseService.getLicensePath() + filename);
                FileUtils.deleteQuietly(dest);
                FileUtils.copyInputStreamToFile(files[0].getInputStream(), dest);
                licenseService.updateLicense();
                logger.info("{}:{}", LogType.UserLog.UPLOAD_LICENSE_FILE.getMessage(), filename);
                logService.log(LogType.UserLog.UPLOAD_LICENSE_FILE);
            }
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logService.log(LogType.UserLog.UPLOAD_LICENSE_FILE_ERROR);
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

}