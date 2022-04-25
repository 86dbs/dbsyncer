package org.dbsyncer.biz.checker.impl.mapping;

import org.dbsyncer.biz.checker.MappingConfigChecker;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.listener.config.ListenerConfig;
import org.dbsyncer.listener.enums.ListenerTypeEnum;
import org.dbsyncer.parser.model.Mapping;
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
        String period = params.get("incrementStrategyTimingPeriodExpression");
        String cron = params.get("incrementStrategyTimingCronExpression");
        String eventFieldName = params.get("incrementStrategyTimingEventFieldName");
        String insert = params.get("incrementStrategyTimingInsert");
        String update = params.get("incrementStrategyTimingUpdate");
        String delete = params.get("incrementStrategyTimingDelete");

        ListenerConfig config = mapping.getListener();
        Assert.notNull(config, "ListenerConfig can not be null.");

        if (StringUtil.isNotBlank(period)) {
            config.setPeriod(NumberUtil.toLong(period, 30));
        }
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