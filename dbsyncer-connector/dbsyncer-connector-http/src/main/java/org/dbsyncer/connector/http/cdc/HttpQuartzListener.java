/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.http.cdc;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.http.constant.HttpConstant;
import org.dbsyncer.connector.http.util.HttpUtil;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.dbsyncer.sdk.listener.AbstractQuartzListener;
import org.dbsyncer.sdk.listener.QuartzFilter;
import org.dbsyncer.sdk.model.Point;
import org.dbsyncer.sdk.model.Table;
import org.dbsyncer.sdk.model.TableGroupQuartzCommand;
import org.dbsyncer.sdk.util.PrimaryKeyUtil;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * CDC-Http 定时监听器
 * 在 checkLastPoint 中一次性根据 snapshot 和当前时间生成时间/日期类占位符的替换值，
 * 与 $pageIndex$、$pageSize$、$cursor$ 在 reader 中统一替换。
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-02-02 00:01
 */
public final class HttpQuartzListener extends AbstractQuartzListener {

    @Override
    public void init() {
        super.init();
        // 初始化游标参数
        getCommands().forEach(cmd -> {
            Properties extInfo = cmd.getTable().getExtInfo();
            String paramsTemplate = extInfo.getProperty(HttpConstant.PARAMS);
            List<String> cursorKeys = new ArrayList<>();
            if (StringUtil.isNotBlank(paramsTemplate)) {
                Properties template = HttpUtil.parse(paramsTemplate);
                for (Map.Entry<Object, Object> entry : template.entrySet()) {
                    String key = (String) entry.getKey();
                    String raw = (String) entry.getValue();
                    String val = raw != null ? StringUtil.trim(raw) : "";
                    if (HttpConstant.CURSOR.equals(val)) {
                        cursorKeys.add(key);
                    }
                }
            }
            cmd.setCursorKeys(cursorKeys);
        });
    }

    @Override
    protected Point checkLastPoint(TableGroupQuartzCommand cmd, int index) {
        Point point = new Point();

        // 从表配置读取 PARAMS 模板，扫描其中的时间/日期类系统占位符
        String paramsTemplate = cmd.getTable() != null && cmd.getTable().getExtInfo() != null
                ? cmd.getTable().getExtInfo().getProperty(HttpConstant.PARAMS)
                : null;
        if (StringUtil.isBlank(paramsTemplate)) {
            return point;
        }

        // 找出模板中出现的 QuartzFilterEnum 占位符（按 index 排序，与 DB 一致）
        List<QuartzFilterEnum> filterEnums = Stream.of(QuartzFilterEnum.values())
                .sorted(Comparator.comparing(QuartzFilterEnum::getIndex))
                .filter(f -> StringUtil.contains(paramsTemplate, f.getType()))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(filterEnums)) {
            return point;
        }

        // 统一生成静态变量：占位符 -> 替换值（时间/日期），并维护 snapshot
        Map<String, Object> staticVars = new LinkedHashMap<>();
        for (QuartzFilterEnum filterEnum : filterEnums) {
            String type = filterEnum.getType();
            QuartzFilter f = filterEnum.getQuartzFilter();
            String key = index + type;

            if (f.begin()) {
                if (!snapshot.containsKey(key)) {
                    Object val = f.getObject();
                    String valueStr = f.toString(val);
                    snapshot.put(key, valueStr);
                    staticVars.put(type, valueStr);
                } else {
                    String valueStr = (String) snapshot.get(key);
                    staticVars.put(type, valueStr);
                    point.setBeginKey(key);
                    point.setBeginValue(f.toString(f.getObject()));
                }
            } else {
                Object val = f.getObject();
                String valueStr = f.toString(val);
                staticVars.put(type, valueStr);
                point.setBeginValue(valueStr);
            }
        }

        point.setCommand(HttpConstant.HTTP_INCREMENT_VARS, JsonUtil.objToJson(staticVars));
        return point;
    }

    @Override
    protected boolean isSupportedCursor(TableGroupQuartzCommand cmd) {
        Table table = cmd.getTable();
        String property = table.getExtInfo().getProperty(HttpConstant.PARAMS);
        return StringUtil.contains(property, HttpConstant.CURSOR);
    }

    @Override
    protected Object[] getLastCursors(TableGroupQuartzCommand cmd, List<Map> data) {
        return PrimaryKeyUtil.getLastCursors(data, cmd.getCursorKeys());
    }
}