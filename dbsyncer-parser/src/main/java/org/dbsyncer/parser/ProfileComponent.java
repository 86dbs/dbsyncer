/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser;

import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.QuartzFilterEnum;
import org.dbsyncer.parser.enums.ConvertEnum;
import org.dbsyncer.parser.model.ConfigModel;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.parser.model.ProjectGroup;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.parser.model.TableGroup;
import org.dbsyncer.parser.model.UserConfig;
import org.dbsyncer.sdk.enums.OperationEnum;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;

import java.util.List;

/**
 * 配置文件组件（system/user/connector/mapping/tableGroup/meta/projectGroup）
 * <p>
 * {@link ConfigConstant}
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
     * 根据ID获取分组配置
     *
     * @param id
     * @return
     */
    ProjectGroup getProjectGroup(String id);

    /**
     * 获取所有的分组配置
     *
     * @return
     */
    List<ProjectGroup> getProjectGroupAll();

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

    // TableGroup
    String addTableGroup(TableGroup model);

    String editTableGroup(TableGroup model);

    void removeTableGroup(String id);

    TableGroup getTableGroup(String tableGroupId);

    List<TableGroup> getTableGroupAll(String mappingId);

    List<TableGroup> getSortedTableGroupAll(String mappingId);

    int getTableGroupCount(String mappingId);

    // Meta
    Meta getMeta(String metaId);

    List<Meta> getMetaAll();

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