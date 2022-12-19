package org.dbsyncer.storage.query.filter;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.dbsyncer.connector.enums.FilterEnum;
import org.dbsyncer.storage.StorageException;
import org.dbsyncer.storage.query.AbstractFilter;

public class StringFilter extends AbstractFilter {

    public StringFilter(String name, String value, boolean enableHighLightSearch) {
        super(name, FilterEnum.EQUAL, value, enableHighLightSearch);
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