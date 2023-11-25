/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.enums.QuartzFilterEnum;
import org.dbsyncer.sdk.enums.OperationEnum;

import java.util.List;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2020-01-19 17:20
 */
public class ConditionVo {

    /**
     * 条件
     */
    private List<OperationEnum> operation;

    /**
     * 系统参数(定时配置)
     */
    private List<QuartzFilterEnum> quartzFilter;

    /**
     * 运算符
     */
    private List<FilterEnum> filter;

    public ConditionVo(List<OperationEnum> operation, List<QuartzFilterEnum> quartzFilter, List<FilterEnum> filter) {
        this.operation = operation;
        this.quartzFilter = quartzFilter;
        this.filter = filter;
    }

    public List<OperationEnum> getOperation() {
        return operation;
    }

    public void setOperation(List<OperationEnum> operation) {
        this.operation = operation;
    }

    public List<QuartzFilterEnum> getQuartzFilter() {
        return quartzFilter;
    }

    public void setQuartzFilter(List<QuartzFilterEnum> quartzFilter) {
        this.quartzFilter = quartzFilter;
    }

    public List<FilterEnum> getFilter() {
        return filter;
    }

    public void setFilter(List<FilterEnum> filter) {
        this.filter = filter;
    }
}