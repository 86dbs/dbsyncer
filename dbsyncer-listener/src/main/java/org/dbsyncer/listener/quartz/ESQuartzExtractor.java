package org.dbsyncer.listener.quartz;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.model.Filter;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.enums.QuartzFilterEnum;

import java.util.*;

/**
 * ES定时抽取
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2021-09-01 20:35
 */
public final class ESQuartzExtractor extends AbstractQuartzExtractor {

    @Override
    protected Point checkLastPoint(Map<String, String> command, int index) {
        // 检查是否存在系统参数
        String filterJson = command.get(ConnectorConstant.OPERTION_QUERY_FILTER);
        if (StringUtil.isBlank(filterJson)) {
            return new Point(command, new ArrayList<>());
        }
        List<Filter> filters = JsonUtil.jsonToArray(filterJson, Filter.class);
        if (CollectionUtils.isEmpty(filters)) {
            return new Point(command, new ArrayList<>());
        }

        // 存在系统参数，替换
        Point point = new Point();
        Set<String> set = new HashSet<>();
        for (Filter f : filters) {
            if (set.contains(f.getValue())) {
                throw new ListenerException(String.format("系统参数%s存在多个.", f.getValue()));
            }
            QuartzFilterEnum filterEnum = QuartzFilterEnum.getQuartzFilterEnum(f.getValue());
            if (null != filterEnum) {
                // 标记防重
                set.add(f.getValue());

                final QuartzFilter quartzFilter = filterEnum.getQuartzFilter();

                // 创建参数索引key
                final String key = index + filterEnum.getType();

                // 开始位置
                if (quartzFilter.begin()) {
                    if (!snapshot.containsKey(key)) {
                        f.setValue(quartzFilter.toString(quartzFilter.getObject()));
                        snapshot.put(key, f.getValue());
                        continue;
                    }

                    // 读取历史增量点
                    f.setValue(snapshot.get(key));
                    point.setBeginKey(key);
                    point.setBeginValue(quartzFilter.toString(quartzFilter.getObject()));
                    continue;
                }
                // 结束位置(刷新)
                f.setValue(quartzFilter.toString(quartzFilter.getObject()));
                point.setBeginValue(f.getValue());
            }
        }
        point.setCommand(ConnectorConstant.OPERTION_QUERY, command.get(ConnectorConstant.OPERTION_QUERY));
        point.setCommand(ConnectorConstant.OPERTION_QUERY_FILTER, JsonUtil.objToJson(filters));
        return point;
    }
}
