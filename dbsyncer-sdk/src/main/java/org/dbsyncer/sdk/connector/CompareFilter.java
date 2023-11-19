package org.dbsyncer.sdk.connector;

public interface CompareFilter {

    boolean compare(String value, String filterValue);

}