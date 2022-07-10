package org.dbsyncer.storage.enums;

import org.dbsyncer.storage.lucene.IndexFieldResolver;

public enum IndexFieldResolverEnum {

    STRING((f) -> f.stringValue()),

    BINARY((f) -> f.binaryValue());

    private IndexFieldResolver indexFieldResolver;

    IndexFieldResolverEnum(IndexFieldResolver indexFieldResolver) {
        this.indexFieldResolver = indexFieldResolver;
    }

    public IndexFieldResolver getIndexFieldResolver() {
        return indexFieldResolver;
    }
}