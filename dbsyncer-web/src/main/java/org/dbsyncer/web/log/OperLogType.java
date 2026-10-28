/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.log;

/**
 * {@link OperLog#type()} 常量，与 {@link org.dbsyncer.parser.LogType} 类型码对齐。
 *
 * @author wuji
 * @version 1.0.0
 */
public final class OperLogType {

    /**
     * 系统-正常
     */
    public static final String SYSTEM_INFO = "10";
    /**
     * 系统-错误
     */
    public static final String SYSTEM_ERROR = "12";

    /**
     * 连接器-新增
     */
    public static final String CONNECTOR_INSERT = "20";
    /**
     * 连接器-修改
     */
    public static final String CONNECTOR_UPDATE = "21";
    /**
     * 连接器-删除
     */
    public static final String CONNECTOR_DELETE = "22";
    /**
     * 连接器-复制
     */
    public static final String CONNECTOR_COPY = "25";

    /**
     * 同步任务-新增
     */
    public static final String MAPPING_INSERT = "30";
    /**
     * 同步任务-修改
     */
    public static final String MAPPING_UPDATE = "31";
    /**
     * 同步任务-删除
     */
    public static final String MAPPING_DELETE = "32";
    /**
     * 同步任务-启动
     */
    public static final String MAPPING_RUNNING = "33";
    /**
     * 同步任务-停止
     */
    public static final String MAPPING_STOP = "34";
    /**
     * 同步任务-清空同步数据
     */
    public static final String MAPPING_CLEAR_DATA = "35";
    /**
     * 同步任务-复制
     */
    public static final String MAPPING_COPY = "36";

    /**
     * 映射关系-新增
     */
    public static final String TABLE_GROUP_INSERT = "40";
    /**
     * 映射关系-修改
     */
    public static final String TABLE_GROUP_UPDATE = "41";
    /**
     * 映射关系-删除
     */
    public static final String TABLE_GROUP_DELETE = "42";
    /**
     * 映射关系-复制
     */
    public static final String TABLE_GROUP_COPY = "45";

    /**
     * 元信息-删除数据
     */
    public static final String META_CLEAR = "51";

    /**
     * 插件-上传
     */
    public static final String PLUGIN_UPDATE = "60";

    /**
     * 配置-导入
     */
    public static final String CACHE_IMPORT = "70";
    /**
     * 配置-导出
     */
    public static final String CACHE_EXPORT = "72";

    /**
     * 用户-新增
     */
    public static final String USER_INSERT = "80";
    /**
     * 用户-修改
     */
    public static final String USER_UPDATE = "81";
    /**
     * 用户-删除
     */
    public static final String USER_DELETE = "82";
    /**
     * 许可-上传
     */
    public static final String USER_UPLOAD_LICENSE = "83";
    /**
     * 许可-激活
     */
    public static final String USER_ACTIVATE_LICENSE = "85";
    /**
     * 许可-删除
     */
    public static final String USER_DELETE_LICENSE = "87";
}
