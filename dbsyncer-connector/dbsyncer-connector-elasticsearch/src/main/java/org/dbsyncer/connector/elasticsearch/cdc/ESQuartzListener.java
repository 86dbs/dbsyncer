/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.elasticsearch.cdc;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.elasticsearch.ElasticsearchConnector;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.dbsyncer.sdk.listener.AbstractQuartzListener;
import org.dbsyncer.sdk.listener.QuartzFilter;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.sdk.model.Point;
import org.dbsyncer.sdk.model.TableGroupQuartzCommand;
import org.elasticsearch.ElasticsearchException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CDC-ES定时监听器
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2021-09-01 20:35
 */
public final class ESQuartzListener extends AbstractQuartzListener {

    @Override
    public Map<String, String> captureSnapshot() {
        List<TableGroupQuartzCommand> cmdList = getCommands();
        if (CollectionUtils.isEmpty(cmdList)) {
            return Collections.emptyMap();
        }
        Map<String, String> captured = new HashMap<>();
        for (int i = 0; i < cmdList.size(); i++) {
            collectBeginQuartzPoint(cmdList.get(i), i, captured);
        }
        return captured;
    }

    /**
     * 全量+增量：以当前时刻冻结 begin 类系统参数（如 $timestamp_begin$），全量结束后增量从该水位继续拉取
     */
    private void collectBeginQuartzPoint(TableGroupQuartzCommand cmd, int index, Map<String, String> captured) {
        Map<String, String> command = cmd.getCommand();
        String filterJson = command.get(ConnectorConstant.OPERTION_QUERY_FILTER);
        if (StringUtil.isBlank(filterJson)) {
            return;
        }
        List<Filter> filters = JsonUtil.jsonToArray(filterJson, Filter.class);
        if (CollectionUtils.isEmpty(filters)) {
            return;
        }
        Set<String> seen = new HashSet<>();
        for (Filter f : filters) {
            String placeholder = f.getValue();
            if (!seen.add(placeholder)) {
                throw new ElasticsearchException(String.format("系统参数%s存在多个.", placeholder));
            }
            QuartzFilterEnum filterEnum = QuartzFilterEnum.getQuartzFilterEnum(placeholder);
            if (filterEnum == null || !filterEnum.getQuartzFilter().begin()) {
                continue;
            }
            QuartzFilter quartzFilter = filterEnum.getQuartzFilter();
            String key = index + filterEnum.getType();
            String value = quartzFilter.toString(quartzFilter.getObject());
            captured.put(key, value);
            snapshot.put(key, value);
        }
    }

    @Override
    protected Point checkLastPoint(TableGroupQuartzCommand cmd, int index) {
        // 检查是否存在系统参数
        Map<String, String> command = cmd.getCommand();
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
                throw new ElasticsearchException(String.format("系统参数%s存在多个.", f.getValue()));
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
                    f.setValue((String) snapshot.get(key));
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
        point.setCommand(ElasticsearchConnector._SOURCE_INDEX, command.get(ElasticsearchConnector._SOURCE_INDEX));
        return point;
    }
}
