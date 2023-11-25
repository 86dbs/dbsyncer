/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.storage.query.filter;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.dbsyncer.sdk.enums.FilterEnum;
import org.dbsyncer.storage.StorageException;
import org.dbsyncer.storage.query.AbstractFilter;

public class StringFilter extends AbstractFilter {

    public StringFilter(String name, FilterEnum filterEnum, String value, boolean enableHighLightSearch) {
        super(name, filterEnum, value, enableHighLightSearch);
    }

    @Override
    public Query newEqual() {
        return new TermQuery(new Term(getName(), getValue()));
    }

    @Override
    public Query newLessThan() {
        throw new StorageException("Unsupported method newLessThan.");
    }
}