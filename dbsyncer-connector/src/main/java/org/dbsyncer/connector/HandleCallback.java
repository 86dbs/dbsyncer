package org.dbsyncer.connector;

import org.dbsyncer.connector.database.DatabaseTemplate;

public interface HandleCallback {

    Object apply(DatabaseTemplate databaseTemplate) throws Exception;

}