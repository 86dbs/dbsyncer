package org.dbsyncer.storage.query;


import org.dbsyncer.connector.enums.OperationEnum;
import org.dbsyncer.connector.model.Filter;

import java.util.ArrayList;
import java.util.List;

public class BooleanQuery {

    private final List<BooleanQuery> clauses = new ArrayList<>();

    private final List<Filter> filters = new ArrayList<>();

    private OperationEnum operationEnum;

    public BooleanQuery add(BooleanQuery booleanQuery) {
        return add(booleanQuery, OperationEnum.AND);
    }

    public BooleanQuery add(BooleanQuery booleanQuery, OperationEnum operationEnum) {
        clauses.add(booleanQuery);
        booleanQuery.setOperationEnum(operationEnum);
        return this;
    }

    public BooleanQuery add(Filter filter) {
        filter.setOperation(OperationEnum.AND.getName());
        filters.add(filter);
        return this;
    }

    public BooleanQuery or(Filter filter) {
        filter.setOperation(OperationEnum.OR.getName());
        filters.add(filter);
        return this;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public OperationEnum getOperationEnum() {
        return operationEnum;
    }

    public void setOperationEnum(OperationEnum operationEnum) {
        this.operationEnum = operationEnum;
    }
}
