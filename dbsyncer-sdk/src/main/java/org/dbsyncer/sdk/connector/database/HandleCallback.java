package org.dbsyncer.sdk.connector.database;

public interface HandleCallback {

    Object apply(DatabaseTemplate databaseTemplate) throws Exception;

}