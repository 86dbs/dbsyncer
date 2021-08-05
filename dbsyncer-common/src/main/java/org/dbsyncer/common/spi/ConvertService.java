package org.dbsyncer.common.spi;

import java.util.List;
import java.util.Map;

/**
 * 插件扩展服务接口
 * <p>全量同步/增量同步,扩展转换</p>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/02/22 20:26
 */
public interface ConvertService {

    /**
     * 全量同步
     *
     * @param source 数据源
     * @param target 目标源
     */
    void convert(List<Map> source, List<Map> target);

    /**
     * 增量同步
     *
     * @param event  事件（INSERT/UPDATE/DELETE）
     * @param source 数据源
     * @param target 目标源
     */
    void convert(String event, Map source, Map target);

    /**
     * 版本号
     *
     * @return
     */
    default String getVersion() {
        return "1.0.0";
    }

    /**
     * 插件名称
     *
     * @return
     */
    default String getName() {
        return getClass().getSimpleName();
    }

}