package org.dbsyncer.sdk.connector.database.sql;

public interface SqlTemplate {
    String getLeftQuotation();
    String getRightQuotation();

    /**
     * 占位符：schema, table, primaryKey condition
     * @return
     */
    default String getDelete(){
        return "DELETE FROM %s.%s WHERE %s";
    }
}
