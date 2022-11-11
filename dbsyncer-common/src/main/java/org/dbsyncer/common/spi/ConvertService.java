package org.dbsyncer.common.spi;

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
     * 全量同步/增量同步
     *
     * @param context 上下文
     */
    void convert(ConvertContext context);

    /**
     * 全量同步/增量同步完成后执行处理
     *
     * @param context 上下文
     */
    default void postProcessAfter(ConvertContext context) {
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