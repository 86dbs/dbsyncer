/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;

import java.util.List;
import java.util.Map;

/**
 * 配置文件组件（system/user/connector/mapping + 枚举查询等）。
 * <p>表映射见 {@link TableGroupProfile}；Meta 见 {@link MetaProfile}；任务运行结果见 {@link TaskProfile}。
 * <p>{@link ConfigConstant}
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-13 20:48
 */
public interface ProfileComponent {

    /**
     * 解析连接器配置为Connector
     *
     * @param json
     * @return
     */
    Connector parseConnector(String json);

    /**
     * 解析配置
     *
     * @param json
     * @param clazz
     * @param <T>
     * @return
     */
    <T> T parseObject(String json, Class<T> clazz);

    /**
     * 添加ConfigModel
     *
     * @param model
     * @return id
     */
    String addConfigModel(ConfigModel model);

    /**
     * 编辑ConfigModel
     *
     * @param model
     * @return id
     */
    String editConfigModel(ConfigModel model);

    /**
     * 刪除ConfigModel
     *
     * @param id
     * @return
     */
    void removeConfigModel(String id);

    /**
     * 获取系统配置
     *
     * @return
     */
    SystemConfig getSystemConfig();

    /**
     * 获取用户配置
     *
     * @return
     */
    UserConfig getUserConfig();

    /**
     * 根据ID获取连接器
     *
     * @param connectorId
     * @return
     */
    Connector getConnector(String connectorId);

    /**
     * 获取所有的连接器
     *
     * @return
     */
    List<Connector> getConnectorAll();

    // Mapping
    Mapping getMapping(String mappingId);

    List<Mapping> getMappingAll();

    /**
     * 构建导出配置快照(直查库)，用于配置导入/导出。
     *
     * @return 快照
     */
    Map<String, Object> getConfigSnapshot();

    /**
     * 获取所有条件类型
     *
     * @return
     */
    List<OperationEnum> getOperationEnumAll();

    /**
     * 获取过滤条件系统参数
     *
     * @return
     */
    List<QuartzFilterEnum> getQuartzFilterEnumAll();

    /**
     * 获取所有运算符类型
     *
     * @return
     */
    List<FilterEnum> getFilterEnumAll();

    /**
     * 获取所有转换类型
     *
     * @return
     */
    List<ConvertEnum> getConvertEnumAll();

    /**
     * 获取所有同步数据状态类型
     *
     * @return
     */
    List<StorageDataStatusEnum> getStorageDataStatusEnumAll();
}
