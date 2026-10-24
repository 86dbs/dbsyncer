/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.notice;

import org.dbsyncer.biz.GroupService;
import org.dbsyncer.biz.NoticeRuleService;
import org.dbsyncer.biz.NoticeStrategyService;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.parser.MappingProfile;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.sdk.model.NoticeConfig;
import org.dbsyncer.sdk.notice.MessageService;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Map;

@Controller
@RequestMapping(value = "/notice")
public class NoticeController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private MessageService messageService;

    @Resource
    private GroupService groupService;

    @Resource
    private MappingProfile mappingProfile;

    @Resource
    private NoticeRuleService noticeRuleService;

    @Resource
    private NoticeStrategyService noticeStrategyService;

    @RequestMapping("")
    public String index(ModelMap model) {
        SystemConfig systemConfig = systemConfigService.getSystemConfig();
        NoticeConfig noticeConfig = systemConfig.getNoticeConfig();
        if (noticeConfig == null) {
            noticeConfig = new NoticeConfig();
            systemConfig.setNoticeConfig(noticeConfig);
        }
        model.put("config", noticeConfig);
        model.put("strategies", noticeStrategyService.listAll());
        model.put("noticeRules", noticeRuleService.listAll());
        model.put("groups", groupService.list());
        Paging<Mapping> mappingPaging = mappingProfile.query(1, 500, null, null);
        model.put("mappings", mappingPaging == null ? new ArrayList<>() : mappingPaging.getData());
        return "notice/list";
    }

    @PostMapping("/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            requirePermission("notice:edit");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(systemConfigService.editNoticeConfig(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/strategy/save")
    @ResponseBody
    public RestResult saveStrategy(HttpServletRequest request) {
        try {
            requirePermission("notice:edit");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(noticeStrategyService.save(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/strategy/remove")
    @ResponseBody
    public RestResult removeStrategy(HttpServletRequest request) {
        try {
            requirePermission("notice:edit");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(noticeStrategyService.remove(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/rule/save")
    @ResponseBody
    public RestResult saveRule(HttpServletRequest request) {
        try {
            requirePermission("notice:edit");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(noticeRuleService.save(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/rule/remove")
    @ResponseBody
    public RestResult removeRule(HttpServletRequest request) {
        try {
            requirePermission("notice:edit");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(noticeRuleService.remove(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/test")
    @ResponseBody
    public RestResult test(HttpServletRequest request) {
        try {
            requirePermission("notice:edit");
            return RestResult.restSuccess(messageService.testSendMessage());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail("测试失败，请检查配置是否正确");
        }
    }

}
