package org.dbsyncer.parser;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 日志类型枚举
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/21 16:19
 */
public interface LogType {

    /**
     * 分类名称
     *
     * @return
     */
    String getName();

    /**
     * 类型
     *
     * @return
     */
    String getType();

    /**
     * 内容
     *
     * @return
     */
    String getMessage();

    /**
     * 全部日志模块枚举；新增模块时在此追加即可。
     */
    LogType[][] ALL = {
            SystemLog.values(),
            ConnectorLog.values(),
            MappingLog.values(),
            TableGroupLog.values(),
            MetaLog.values(),
            PluginLog.values(),
            CacheLog.values(),
            UserLog.values(),
            LoginLog.values()
    };

    /**
     * 按 type 码解析模块名；未匹配时返回原 type。
     */
    static String getNameByType(String type) {
        LogType matched = findByType(type);
        return matched == null ? type : matched.getName();
    }

    /**
     * 按 type 码查找枚举；未匹配返回 null。
     */
    static LogType findByType(String type) {
        if (type == null) {
            return null;
        }
        for (LogType[] group : ALL) {
            LogType matched = match(group, type);
            if (matched != null) {
                return matched;
            }
        }
        return null;
    }

    static LogType match(LogType[] values, String type) {
        for (LogType value : values) {
            if (type.equals(value.getType())) {
                return value;
            }
        }
        return null;
    }

    /**
     * 操作日志可选模块名（排除登录），按 {@link #ALL} 顺序去重。
     * <p>同组共享 {@link #getName()}，取组首项即可。
     */
    static List<String> listOperModuleNames() {
        return Arrays.stream(ALL)
                .filter(group -> group.length > 0 && !(group[0] instanceof LoginLog))
                .map(group -> group[0].getName())
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * 按模块名汇总类型码（逗号拼接），供 IN 查询；未匹配返回 {@code null}。
     */
    static String joinTypesByModuleName(String moduleName) {
        if (moduleName == null || moduleName.isEmpty()) {
            return null;
        }
        String codes = Arrays.stream(ALL)
                .filter(group -> group.length > 0 && !(group[0] instanceof LoginLog)
                        && moduleName.equals(group[0].getName()))
                .flatMap(Arrays::stream)
                .map(LogType::getType)
                .collect(Collectors.joining(","));
        return codes.isEmpty() ? null : codes;
    }

    /**
     * 系统日志1
     */
    enum SystemLog implements LogType {

        /**
         * 正常
         */
        INFO("10", "正常"),
        /**
         * 警告
         */
        WARN("11", "警告"),
        /**
         * 错误
         */
        ERROR("12", "错误");

        private String type;
        private String message;

        SystemLog(String type, String message) {
            this.type = type;
            this.message = message;
        }

        @Override
        public String getName() {
            return "系统日志";
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

    /**
     * 连接器2
     */
    enum ConnectorLog implements LogType {

        INSERT("20", "新增"), UPDATE("21", "修改"), DELETE("22", "删除"), FAILED("23", "连接失败"), COPY("25", "复制");

        private String type;
        private String message;

        ConnectorLog(String type, String message) {
            this.type = type;
            this.message = message;
        }

        @Override
        public String getName() {
            return "连接器";
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

    /**
     * 驱动3
     */
    enum MappingLog implements LogType {

        INSERT("30", "新增"), UPDATE("31", "修改"), DELETE("32", "删除"), RUNNING("33", "启动"), STOP("34", "停止"), CLEAR_DATA("35", "清空同步数据"), COPY("36", "复制");

        private String type;
        private String message;

        MappingLog(String type, String message) {
            this.type = type;
            this.message = message;
        }

        @Override
        public String getName() {
            return "驱动";
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

    /**
     * 映射关系4
     */
    enum TableGroupLog implements LogType {

        INSERT("40", "新增"), UPDATE("41", "修改"), DELETE("42", "删除"), INCREMENT_FAILED("43", "增量同步异常"), FULL_FAILED("44", "全量同步异常"), COPY("45", "复制");

        private String type;
        private String message;

        TableGroupLog(String type, String message) {
            this.type = type;
            this.message = message;
        }

        @Override
        public String getName() {
            return "映射关系";
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

    /**
     * 元信息5
     */
    enum MetaLog implements LogType {

        DELETE("50", "删除"), CLEAR("51", "删除数据");

        private String type;
        private String message;

        MetaLog(String type, String message) {
            this.type = type;
            this.message = message;
        }

        @Override
        public String getName() {
            return "元信息";
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

    /**
     * 插件信息6
     */
    enum PluginLog implements LogType {

        UPDATE("60", "上传成功"), CHECK_ERROR("61", "格式不正确");

        private String type;
        private String message;

        PluginLog(String type, String message) {
            this.type = type;
            this.message = message;
        }

        @Override
        public String getName() {
            return "插件";
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

    /**
     * 缓存配置信息7
     */
    enum CacheLog implements LogType {

        IMPORT("70", "导入配置"), IMPORT_ERROR("71", "导入配置异常"), EXPORT("72", "导出配置");

        private String type;
        private String message;

        CacheLog(String type, String message) {
            this.type = type;
            this.message = message;
        }

        @Override
        public String getName() {
            return "配置";
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

    /**
     * 用户日志8
     */
    enum UserLog implements LogType {

        INSERT("80", "新增"), UPDATE("81", "修改"), DELETE("82", "删除"), UPLOAD_LICENSE_FILE("83", "上传授权许可文件"), UPLOAD_LICENSE_FILE_ERROR("84", "上传授权许可文件失败"), ACTIVATE_FREE_LICENSE_FILE("85", "激活授权许可文件"),
        ACTIVATE_FREE_LICENSE_FILE_ERROR("86", "激活授权许可文件失败"), DELETE_LICENSE_FILE("87", "删除授权许可文件"), DELETE_LICENSE_FILE_ERROR("88", "删除授权许可文件失败");

        private String type;
        private String message;

        UserLog(String type, String message) {
            this.type = type;
            this.message = message;
        }

        @Override
        public String getName() {
            return "用户日志";
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

    /**
     * 登录日志9
     */
    enum LoginLog implements LogType {

        SUCCESS("90", "登录成功"), FAIL("91", "登录失败");

        private String type;
        private String message;

        LoginLog(String type, String message) {
            this.type = type;
            this.message = message;
        }

        @Override
        public String getName() {
            return "登录日志";
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public String getMessage() {
            return message;
        }
    }

}