/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.query.filter;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.storage.query.AbstractFilter;

public class LongFilter extends AbstractFilter {

    public LongFilter(String name, FilterEnum filterEnum, long value) {
        super(name, filterEnum, value);
    }

    @Override
    public Query newEqual() {
        return LongPoint.newSetQuery(getName(), NumberUtil.toLong(getValue()));
    }

    @Override
    public Query newLessThan() {
        return LongPoint.newRangeQuery(getName(), Long.MIN_VALUE, NumberUtil.toLong(getValue()));
    }
}