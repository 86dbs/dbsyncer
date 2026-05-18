/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.impl;

import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.sdk.filter.AbstractFilter;
import org.dbsyncer.storage.StorageException;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-11 22:27
 */
public class DiskQueryHelper {

    public static Query newEqual(AbstractFilter filter) {
        Query query = null;
        switch (filter.getFilterTypeEnum()) {
            case STRING:
                query = new TermQuery(new Term(filter.getName(), filter.getValue()));
                break;
            case LONG:
                query = LongPoint.newSetQuery(filter.getName(), NumberUtil.toLong(filter.getValue()));
                break;
            case INT:
                query = IntPoint.newSetQuery(filter.getName(), NumberUtil.toInt(filter.getValue()));
                break;
        }
        if (query == null) {
            throw new StorageException("Unsupported method newEqual.");
        }
        return query;
    }

    /**
     * Lucene {@link LongPoint#newRangeQuery} / {@link IntPoint#newRangeQuery} 两端均为<strong>闭区间</strong>，
     * 因此严格 {@link FilterEnum#GT} / {@link FilterEnum#LT} 不能与 {@link FilterEnum#GT_AND_EQUAL} / {@link FilterEnum#LT_AND_EQUAL} 共用同一区间。
     */
    public static Query newLessThan(AbstractFilter filter, FilterEnum op) {
        switch (filter.getFilterTypeEnum()) {
            case LONG:
                long longBound = NumberUtil.toLong(filter.getValue());
                if (op == FilterEnum.LT) {
                    if (longBound <= Long.MIN_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    return LongPoint.newRangeQuery(filter.getName(), Long.MIN_VALUE, longBound - 1);
                }
                return LongPoint.newRangeQuery(filter.getName(), Long.MIN_VALUE, longBound);
            case INT:
                int intBound = NumberUtil.toInt(filter.getValue());
                if (op == FilterEnum.LT) {
                    if (intBound <= Integer.MIN_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    return IntPoint.newRangeQuery(filter.getName(), Integer.MIN_VALUE, intBound - 1);
                }
                return IntPoint.newRangeQuery(filter.getName(), Integer.MIN_VALUE, intBound);
            default:
                throw new StorageException("Unsupported method newLessThan.");
        }
    }

    public static Query newGreaterThan(AbstractFilter filter, FilterEnum op) {
        switch (filter.getFilterTypeEnum()) {
            case LONG:
                long longBound = NumberUtil.toLong(filter.getValue());
                if (op == FilterEnum.GT) {
                    if (longBound >= Long.MAX_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    return LongPoint.newRangeQuery(filter.getName(), longBound + 1, Long.MAX_VALUE);
                }
                return LongPoint.newRangeQuery(filter.getName(), longBound, Long.MAX_VALUE);
            case INT:
                int intBound = NumberUtil.toInt(filter.getValue());
                if (op == FilterEnum.GT) {
                    if (intBound >= Integer.MAX_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    return IntPoint.newRangeQuery(filter.getName(), intBound + 1, Integer.MAX_VALUE);
                }
                return IntPoint.newRangeQuery(filter.getName(), intBound, Integer.MAX_VALUE);
            default:
                throw new StorageException("Unsupported method newGreaterThan.");
        }
    }
}
