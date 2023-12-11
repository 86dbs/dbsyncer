/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.impl;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.sdk.filter.AbstractFilter;
import org.dbsyncer.storage.StorageException;

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

    public static Query newLessThan(AbstractFilter filter) {
        Query query = null;
        switch (filter.getFilterTypeEnum()) {
            case LONG:
                query = LongPoint.newRangeQuery(filter.getName(), Long.MIN_VALUE, NumberUtil.toLong(filter.getValue()));
                break;
            case INT:
                query = IntPoint.newRangeQuery(filter.getName(), Integer.MIN_VALUE, NumberUtil.toInt(filter.getValue()));
                break;
        }
        if (query == null) {
            throw new StorageException("Unsupported method newLessThan.");
        }
        return query;
    }

}