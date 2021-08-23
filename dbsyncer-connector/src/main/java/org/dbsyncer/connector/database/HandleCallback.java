package org.dbsyncer.connector.database;

public interface HandleCallback {

    Object apply(DatabaseTemplate databaseTemplate) throws Exception;

}