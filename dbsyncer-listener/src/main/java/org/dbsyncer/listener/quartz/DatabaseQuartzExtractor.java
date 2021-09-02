package org.dbsyncer.listener.quartz;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.listener.enums.QuartzFilterEnum;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 关系型数据库定时抽取
 *
 * @version 1.0.0
 * @Author AE86
 * @Date 2021-09-01 20:35
 */
public final class DatabaseQuartzExtractor extends AbstractQuartzExtractor {

    @Override
    protected Point checkLastPoint(Map<String, String> command, int index) {
        // 检查是否存在系统参数
        final String query = command.get(ConnectorConstant.OPERTION_QUERY);
        List<QuartzFilterEnum> filterEnums = Stream.of(QuartzFilterEnum.values()).filter(f -> {
            Assert.isTrue(appearNotMoreThanOnce(query, f.getType()), String.format("系统参数%s存在多个.", f.getType()));
            return StringUtil.contains(query, f.getType());
        }).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(filterEnums)) {
            return new Point(command, new ArrayList<>());
        }

        Point point = new Point();
        // 存在系统参数，替换
        String replaceQuery = query;
        for (QuartzFilterEnum quartzFilter : filterEnums) {
            final String type = quartzFilter.getType();
            final QuartzFilter f = quartzFilter.getQuartzFilter();

            // 替换字符
            replaceQuery = StringUtil.replace(replaceQuery, "'" + type + "'", "?");

            // 创建参数索引key
            final String key = index + type;

            // 开始位置
            if(f.begin()){
                if (!snapshot.containsKey(key)) {
                    final Object val = f.getObject();
                    point.addArg(val);
                    snapshot.put(key, f.toString(val));
                    continue;
                }

                // 读取历史增量点
                Object val = f.getObject(snapshot.get(key));
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

        return point;
    }

    private boolean appearNotMoreThanOnce(String str, String searchStr) {
        return StringUtil.indexOf(str, searchStr) == StringUtil.lastIndexOf(str, searchStr);
    }
}
