/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.sdk.listener;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.dbsyncer.sdk.model.Point;
import org.dbsyncer.sdk.model.TableGroupQuartzCommand;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 关系型数据库定时抽取
 *
 * @version 1.0.0
 * @author AE86
 * @date 2021-09-01 20:35
 */
public final class DatabaseQuartzListener extends AbstractQuartzListener {

    @Override
    protected Point checkLastPoint(TableGroupQuartzCommand cmd, int index) {
        // 检查是否存在系统参数
        Map<String, String> command = cmd.getCommand();
        final String query = command.get(ConnectorConstant.OPERTION_QUERY);

        AtomicBoolean reversed = new AtomicBoolean();
        AtomicLong lastIndex = new AtomicLong();
        List<QuartzFilterEnum> filterEnums = Stream.of(QuartzFilterEnum.values()).sorted(Comparator.comparing(QuartzFilterEnum::getIndex)).filter(f-> {
            int currentIndex = StringUtil.indexOf(query, f.getType());
            Assert.isTrue((currentIndex == StringUtil.lastIndexOf(query, f.getType())), String.format("系统参数%s存在多个.", f.getType()));
            boolean exist = StringUtil.contains(query, f.getType());
            if (exist && !reversed.get()) {
                reversed.set(lastIndex.get() > currentIndex);
                lastIndex.set(currentIndex);
            }
            return exist;
        }).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(filterEnums)) {
            return new Point(command, new ArrayList<>());
        }

        Point point = new Point();
        // 存在系统参数，替换
        String replaceQuery = query;
        String replaceQueryCursor = command.get(ConnectorConstant.OPERTION_QUERY_CURSOR);
        for (QuartzFilterEnum quartzFilter : filterEnums) {
            final String type = quartzFilter.getType();
            final QuartzFilter f = quartzFilter.getQuartzFilter();

            // 替换字符
            replaceQuery = replaceType(replaceQuery, type);
            replaceQueryCursor = replaceType(replaceQueryCursor, type);

            // 创建参数索引key
            final String key = index + type;

            // 开始位置
            if (f.begin()) {
                if (!snapshot.containsKey(key)) {
                    final Object val = f.getObject();
                    point.addArg(val);
                    snapshot.put(key, f.toString(val));
                    continue;
                }

                // 读取历史增量点
                Object val = f.getObject((String) snapshot.get(key));
                point.addArg(val);
                point.setBeginKey(key);
                point.setBeginValue(f.toString(f.getObject()));
                continue;
            }
            // 结束位置(刷新)
            Object val = f.getObject();
            point.addArg(val);
            point.setBeginValue(f.toString(val));
        }
        point.setCommand(ConnectorConstant.OPERTION_QUERY, replaceQuery);
        if (StringUtil.isNotBlank(replaceQueryCursor)) {
            point.setCommand(ConnectorConstant.OPERTION_QUERY_CURSOR, replaceQueryCursor);
        }
        if (reversed.get()) {
            point.reverseArgs();
        }

        return point;
    }

    private String replaceType(String replaceQuery, String type) {
        if (StringUtil.isNotBlank(replaceQuery)) {
            replaceQuery = StringUtil.replace(replaceQuery, "'" + type + "'", "?");
            return StringUtil.replace(replaceQuery, type, "?");
        }
        return replaceQuery;
    }

}