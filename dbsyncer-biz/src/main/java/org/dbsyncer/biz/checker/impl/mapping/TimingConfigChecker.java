package org.dbsyncer.biz.checker.impl.mapping;

import org.dbsyncer.biz.checker.MappingConfigChecker;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.sdk.config.ListenerConfig;
import org.dbsyncer.sdk.enums.ListenerTypeEnum;

import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * 定时配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/1/8 15:17
 */
@Component
public class TimingConfigChecker implements MappingConfigChecker {

    @Override
    public void modify(Mapping mapping, Map<String, String> params) {
        String cron = params.get("timingCronExpression");
        String eventFieldName = params.get("timingEventFieldName");
        String insert = params.get("timingInsert");
        String update = params.get("timingUpdate");
        String delete = params.get("timingDelete");

        ListenerConfig config = mapping.getListener();
        Assert.notNull(config, "ListenerConfig can not be null.");

        if (StringUtil.isNotBlank(cron)) {
            config.setCron(cron);
        }
        config.setEventFieldName(eventFieldName);
        if (StringUtil.isNotBlank(insert)) {
            config.setInsert(insert);
        }
        if (StringUtil.isNotBlank(update)) {
            config.setUpdate(update);
        }
        if (StringUtil.isNotBlank(delete)) {
            config.setDelete(delete);
        }

        config.setListenerType(ListenerTypeEnum.TIMING.getType());
        mapping.setListener(config);
    }
}
