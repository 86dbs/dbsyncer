/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.web.controller.config;

import org.dbsyncer.biz.ParamConfigService;
import org.dbsyncer.biz.vo.ParamConfigVo;
import org.dbsyncer.biz.vo.RestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.List;

/**
 * 参数配置控制器
 *
 * @author AE86
 * @version 1.0.0
 * @date 2025/01/08
 */
@Controller
@RequestMapping("/param")
public class ParamController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private ParamConfigService paramConfigService;

    /**
     * 获取参数选项
     */
    @GetMapping("/getParamOptions.json")
    @ResponseBody
    public RestResult getParamOptions() {
        try {
            logger.debug("获取参数选项");

            List<ParamConfigVo> paramConfigs = paramConfigService.getAllParamConfigs();

            logger.debug("返回参数选项数量: {}", paramConfigs.size());
            return RestResult.restSuccess(paramConfigs);
        } catch (Exception e) {
            logger.error("获取参数选项失败: {}", e.getMessage(), e);
            return RestResult.restFail("获取参数选项失败: " + e.getMessage());
        }
    }
}