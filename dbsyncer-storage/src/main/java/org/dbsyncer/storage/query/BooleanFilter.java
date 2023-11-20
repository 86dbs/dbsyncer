package org.dbsyncer.storage.query;

import org.dbsyncer.sdk.enums.OperationEnum;

import java.util.ArrayList;
import java.util.List;

public class BooleanFilter {

    private final List<BooleanFilter> clauses = new ArrayList<>();

    private final List<AbstractFilter> filters = new ArrayList<>();

    private OperationEnum operationEnum;

    public BooleanFilter add(BooleanFilter booleanFilter, OperationEnum operationEnum) {
        clauses.add(booleanFilter);
        booleanFilter.setOperationEnum(operationEnum);
        return this;
    }

    public BooleanFilter add(AbstractFilter filter) {
        filter.setOperation(OperationEnum.AND.getName());
        filters.add(filter);
        return this;
    }

    public BooleanFilter or(AbstractFilter filter) {
        filter.setOperation(OperationEnum.OR.getName());
        filters.add(filter);
        return this;
    }

    public List<AbstractFilter> getFilters() {
        return filters;
    }

    public List<BooleanFilter> getClauses() {
        return clauses;
    }

    public OperationEnum getOperationEnum() {
        return operationEnum;
    }

    public void setOperationEnum(OperationEnum operationEnum) {
        this.operationEnum = operationEnum;
    }
}
