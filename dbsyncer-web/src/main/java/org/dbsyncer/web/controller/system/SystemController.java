/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.web.controller.system;

import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.parser.SystemConfigProfile;
import org.dbsyncer.web.controller.BaseController;
import org.dbsyncer.web.log.OperLog;
import org.dbsyncer.web.log.OperLogType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@Controller
@RequestMapping(value = "/system")
public class SystemController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private SystemConfigProfile systemConfigProfile;

    @RequestMapping("")
    public String index(ModelMap model) {
        model.put("config", systemConfigService.getSystemConfigVo());
        initEditionInfo(model);
        return "system/list";
    }

    @OperLog(type = OperLogType.SYSTEM_INFO, message = "修改系统配置")
    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            requirePermission("system:edit");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(systemConfigService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @OperLog(type = OperLogType.SYSTEM_INFO, message = "生成RSA密钥")
    @PostMapping("/generateRSA")
    @ResponseBody
    public RestResult generateRSA(HttpServletRequest request, @RequestParam(value = "keyLength") int keyLength) {
        try {
            requirePermission("system:edit");
            return RestResult.restSuccess(systemConfigService.createRSAConfig(keyLength));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    /**
     * 生成 API 密钥（OpenAPI 登录凭证）
     */
    @OperLog(type = OperLogType.SYSTEM_INFO, message = "生成API密钥")
    @PostMapping("/generateApiSecret")
    @ResponseBody
    public RestResult generateApiSecret(HttpServletRequest request) {
        try {
            requirePermission("system:edit");
            return RestResult.restSuccess(systemConfigProfile.generateApiSecret());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

}