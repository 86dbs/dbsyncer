package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.RestResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 目标表不存在异常
 * 用于标识目标表不存在，需要用户确认是否创建
 */
public class TargetTableNotExistsException extends BizException {

    /**
     * 错误码：用于前端识别异常类型
     */
    public static final String ERROR_CODE = "TARGET_TABLE_NOT_EXISTS";

    /**
     * 所有缺失的表映射列表
     * 每个 Map 包含 sourceTable 和 targetTable 两个键
     */
    private List<Map<String, String>> missingTables;

    /**
     * 构造函数
     *
     * @param message 异常消息
     * @param missingTables 缺失的表映射列表，每个 Map 包含 sourceTable 和 targetTable 两个键
     */
    public TargetTableNotExistsException(String message, List<Map<String, String>> missingTables) {
        super(message);
        this.missingTables = missingTables != null ? missingTables : new ArrayList<>();
    }

    public String getErrorCode() {
        return ERROR_CODE;
    }

    /**
     * 获取所有缺失的表映射列表
     */
    public List<Map<String, String>> getMissingTables() {
        return missingTables;
    }

    /**
     * 判断是否有多个缺失的表
     */
    public boolean hasMultipleTables() {
        return missingTables != null && missingTables.size() > 1;
    }

    /**
     * 将异常转换为 RestResult 响应对象
     * 封装了异常信息到 RestResult 的转换逻辑
     *
     * @return RestResult 包含错误码、消息和缺失表列表的响应对象
     */
    public RestResult toRestResult() {
        Map<String, Object> errorInfo = new HashMap<>();
        errorInfo.put("errorCode", ERROR_CODE);
        errorInfo.put("message", getMessage());
        errorInfo.put("missingTables", missingTables);
        
        return RestResult.restFail(errorInfo, 400);
    }
}

