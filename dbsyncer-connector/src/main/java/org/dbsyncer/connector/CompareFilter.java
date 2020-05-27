package org.dbsyncer.connector;

public interface CompareFilter {

    boolean compare(String value, String filterValue);

}