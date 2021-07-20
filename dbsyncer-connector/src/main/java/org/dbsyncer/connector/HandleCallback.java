package org.dbsyncer.connector;

import java.sql.Connection;

public interface HandleCallback {

    Object apply(Connection connection) throws Exception;

}