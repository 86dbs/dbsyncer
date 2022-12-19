package org.dbsyncer.storage.query.filter;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.search.Query;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.storage.query.AbstractFilter;

public class IntFilter extends AbstractFilter {

    public IntFilter(String name, int value) {
        super(name, FilterEnum.EQUAL, value);
    }

    public IntFilter(String name, FilterEnum filterEnum, int value) {
        super(name, filterEnum, value);
    }

    @Override
    public Query newEqual() {
        return IntPoint.newSetQuery(getName(), NumberUtil.toInt(getValue()));
    }

    @Override
    public Query newLessThan() {
        return IntPoint.newRangeQuery(getName(), Integer.MIN_VALUE, NumberUtil.toInt(getValue()));
    }
}