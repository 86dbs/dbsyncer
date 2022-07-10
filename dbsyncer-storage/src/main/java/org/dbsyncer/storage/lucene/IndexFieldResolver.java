package org.dbsyncer.storage.lucene;

import org.apache.lucene.index.IndexableField;

public interface IndexFieldResolver {

    Object getValue(IndexableField field);

}