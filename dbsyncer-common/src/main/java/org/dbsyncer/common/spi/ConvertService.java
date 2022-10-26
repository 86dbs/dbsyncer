package org.dbsyncer.common.spi;

import org.dbsyncer.common.model.FullConvertContext;
import org.dbsyncer.common.model.IncrementConvertContext;

import java.sql.Connection;

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
     * @author wangxiri
     * @date 2022/10/25
     * 数据插入后处理接口---全量更新未测试
     * @param context 上下文
     * @param connection 目标数据库连接
     * @param targetTableName 数据更新表名
     *

     */
    void AfterConvert(FullConvertContext context,Connection connection,String targetTableName);

    /**
     * 增量同步
     * @param context 上下文
     */
    void convert(IncrementConvertContext context);


    /**
     *  @author wangxiri
     * 数据插入后处理接口
     * @date 2022/10/25
     * @param context 上下文
     * @param connection 目标数据库连接
     * @param targetTableName 数据更新表名
     *
     */
    void AfterConvert(IncrementConvertContext context, Connection connection,String targetTableName);

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