/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.notice;

import org.dbsyncer.biz.GroupService;
import org.dbsyncer.biz.NoticeMentionRuleService;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.parser.MappingProfile;
import org.dbsyncer.parser.message.DefaultNoticeStrategy;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.sdk.enums.NoticeTypeEnum;
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
import java.util.LinkedHashMap;
import java.util.List;
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
    private NoticeMentionRuleService noticeMentionRuleService;

    @RequestMapping("")
    public String index(ModelMap model) {
        SystemConfig systemConfig = systemConfigService.getSystemConfig();
        NoticeConfig noticeConfig = systemConfig.getNoticeConfig();
        if (noticeConfig == null) {
            noticeConfig = new NoticeConfig();
            systemConfig.setNoticeConfig(noticeConfig);
        }
        if (CollectionUtils.isEmpty(noticeConfig.getStrategies())) {
            noticeConfig.setStrategies(DefaultNoticeStrategy.allDefs());
        }
        model.put("config", noticeConfig);
        model.put("mentionRules", noticeMentionRuleService.listAll());
        model.put("groups", groupService.list());
        Paging<Mapping> mappingPaging = mappingProfile.query(1, 500, null, null);
        model.put("mappings", mappingPaging == null ? new ArrayList<>() : mappingPaging.getData());
        model.put("noticeTypeOptions", buildNoticeTypeOptions());
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

    @PostMapping("/mention/save")
    @ResponseBody
    public RestResult saveMention(HttpServletRequest request) {
        try {
            requirePermission("notice:edit");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(noticeMentionRuleService.save(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/mention/remove")
    @ResponseBody
    public RestResult removeMention(HttpServletRequest request) {
        try {
            requirePermission("notice:edit");
            Map<String, String> params = getParams(request);
            return RestResult.restSuccess(noticeMentionRuleService.remove(params));
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

    private List<Map<String, String>> buildNoticeTypeOptions() {
        List<Map<String, String>> options = new ArrayList<>();
        putType(options, NoticeTypeEnum.CONNECTOR_OFFLINE, "连接离线");
        putType(options, NoticeTypeEnum.MAPPING_ERROR, "同步任务失败");
        putType(options, NoticeTypeEnum.MAPPING_STOP, "手动停止同步任务");
        putType(options, NoticeTypeEnum.VALIDATE_SYNC_FAIL, "订正校验失败");
        putType(options, NoticeTypeEnum.LICENSE_EXCEPTION, "授权异常");
        putType(options, NoticeTypeEnum.LICENSE_EXPIRE_REMIND, "授权到期提醒");
        putType(options, NoticeTypeEnum.LICENSE_EXPIRED, "授权已过期");
        return options;
    }

    private void putType(List<Map<String, String>> options, NoticeTypeEnum type, String label) {
        Map<String, String> item = new LinkedHashMap<>();
        item.put("code", type.name());
        item.put("label", label);
        options.add(item);
    }
}
