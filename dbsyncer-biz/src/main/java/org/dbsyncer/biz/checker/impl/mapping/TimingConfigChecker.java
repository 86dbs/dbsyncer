/**
 * Alipay.com Inc. Copyright (c) 2004-2020 All Rights Reserved.
 */
package org.dbsyncer.biz.checker.impl.mapping;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.biz.checker.MappingConfigChecker;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.config.TimingListenerConfig;
import org.dbsyncer.parser.model.Mapping;
import org.springframework.stereotype.Component;

import java.util.Arrays;
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
        String cron = params.get("incrementStrategyTimingCronExpression");
        String eventFieldName = params.get("incrementStrategyTimingEventFieldName");
        String insert = params.get("incrementStrategyTimingInsert");
        String update = params.get("incrementStrategyTimingUpdate");
        String delete = params.get("incrementStrategyTimingDelete");

        TimingListenerConfig config = new TimingListenerConfig();
        if (StringUtils.isNotBlank(cron)) {
            config.setCronExpression(cron);
        }
        if (StringUtils.isNotBlank(eventFieldName)) {
            config.setEventFieldName(eventFieldName);
        }
        if (StringUtils.isNotBlank(insert)) {
            config.setInsert(Arrays.asList(insert.split(",")));
        }
        if (StringUtils.isNotBlank(update)) {
            config.setUpdate(Arrays.asList(update.split(",")));
        }
        if (StringUtils.isNotBlank(delete)) {
            config.setDelete(Arrays.asList(delete.split(",")));
        }

        mapping.setListener(config);
    }

}