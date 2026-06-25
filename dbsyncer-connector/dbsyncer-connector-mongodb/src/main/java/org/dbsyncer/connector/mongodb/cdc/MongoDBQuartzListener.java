/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.mongodb.cdc;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.mongodb.MongoDBConnector;
import org.dbsyncer.connector.mongodb.MongoDBException;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.dbsyncer.sdk.listener.AbstractQuartzListener;
import org.dbsyncer.sdk.listener.QuartzFilter;
import org.dbsyncer.sdk.model.Filter;
import org.dbsyncer.sdk.model.Point;
import org.dbsyncer.sdk.model.TableGroupQuartzCommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 20:00
 */
public final class MongoDBQuartzListener extends AbstractQuartzListener {

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
        Set<String> seenBeginPlaceholder = new HashSet<>();
        for (Filter f : filters) {
            String placeholder = f.getValue();
            QuartzFilterEnum filterEnum = QuartzFilterEnum.getQuartzFilterEnum(placeholder);
            if (filterEnum == null) {
                continue;
            }
            if (!seenBeginPlaceholder.add(placeholder)) {
                throw new MongoDBException(String.format("系统参数%s存在多个.", placeholder));
            }
            if (!filterEnum.getQuartzFilter().begin()) {
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
        Map<String, String> command = cmd.getCommand();
        String filterJson = command.get(ConnectorConstant.OPERTION_QUERY_FILTER);
        if (StringUtil.isBlank(filterJson)) {
            return new Point(command, new ArrayList<>());
        }
        List<Filter> filters = JsonUtil.jsonToArray(filterJson, Filter.class);
        if (CollectionUtils.isEmpty(filters)) {
            return new Point(command, new ArrayList<>());
        }

        Point point = new Point();
        Set<String> set = new HashSet<>();
        for (Filter f : filters) {
            if (set.contains(f.getValue())) {
                throw new MongoDBException(String.format("系统参数%s存在多个.", f.getValue()));
            }
            QuartzFilterEnum filterEnum = QuartzFilterEnum.getQuartzFilterEnum(f.getValue());
            if (filterEnum != null) {
                set.add(f.getValue());
                QuartzFilter quartzFilter = filterEnum.getQuartzFilter();
                String key = index + filterEnum.getType();
                if (quartzFilter.begin()) {
                    if (!snapshot.containsKey(key)) {
                        f.setValue(quartzFilter.toString(quartzFilter.getObject()));
                        snapshot.put(key, f.getValue());
                        continue;
                    }
                    f.setValue((String) snapshot.get(key));
                    point.setBeginKey(key);
                    point.setBeginValue(quartzFilter.toString(quartzFilter.getObject()));
                    continue;
                }
                f.setValue(quartzFilter.toString(quartzFilter.getObject()));
                point.setBeginValue(f.getValue());
            }
        }
        point.setCommand(ConnectorConstant.OPERTION_QUERY, command.get(ConnectorConstant.OPERTION_QUERY));
        point.setCommand(ConnectorConstant.OPERTION_QUERY_FILTER, JsonUtil.objToJson(filters));
        point.setCommand(MongoDBConnector.SOURCE_COLLECTION, command.get(MongoDBConnector.SOURCE_COLLECTION));
        point.setCommand(MongoDBConnector.DATABASE, command.get(MongoDBConnector.DATABASE));
        return point;
    }
}
