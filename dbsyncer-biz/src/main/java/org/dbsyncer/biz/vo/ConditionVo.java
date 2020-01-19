package org.dbsyncer.biz.vo;

import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.connector.enums.OperationEnum;

import java.util.List;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2020/01/19 17:20
 */
public class ConditionVo {

    /**
     * 条件
     */
    private List<OperationEnum> operation;

    /**
     * 运算符
     */
    private List<FilterEnum> filter;

    public ConditionVo(List<OperationEnum> operation, List<FilterEnum> filter) {
        this.operation = operation;
        this.filter = filter;
    }

    public List<OperationEnum> getOperation() {
        return operation;
    }

    public void setOperation(List<OperationEnum> operation) {
        this.operation = operation;
    }

    public List<FilterEnum> getFilter() {
        return filter;
    }

    public void setFilter(List<FilterEnum> filter) {
        this.filter = filter;
    }
}