package org.dbsyncer.storage.enums;

import org.dbsyncer.storage.lucene.IndexFieldResolver;

public enum IndexFieldResolverEnum {

    LONG((f) -> f.numericValue().longValue()),

    INT((f) -> f.numericValue().intValue()),

    STRING((f) -> f.stringValue()),

    BINARY((f) -> f.binaryValue().bytes);

    private IndexFieldResolver indexFieldResolver;

    IndexFieldResolverEnum(IndexFieldResolver indexFieldResolver) {
        this.indexFieldResolver = indexFieldResolver;
    }

    public IndexFieldResolver getIndexFieldResolver() {
        return indexFieldResolver;
    }
}