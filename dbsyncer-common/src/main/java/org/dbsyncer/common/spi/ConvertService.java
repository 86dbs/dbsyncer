package org.dbsyncer.common.spi;

import org.dbsyncer.common.model.FullConvertContext;
import org.dbsyncer.common.model.IncrementConvertContext;

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
     * @param context 上下文
     */
    void convert(FullConvertContext context);

    /**
     * 增量同步
     *
     * @param context 上下文
     */
    void convert(IncrementConvertContext context);

    /**
     * 数据插入后处理接口
     *
     * @param context 上下文
     * @author wangxiri
     * @date 2022/10/25
     */
    default void postProcessAfter(IncrementConvertContext context) {
    }

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