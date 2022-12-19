package org.dbsyncer.manager.command;

import org.dbsyncer.manager.ManagerException;

public interface Persistence {

    default boolean addConfig() {
        throw new ManagerException("Unsupported method addConfig");
    }

    default boolean editConfig() {
        throw new ManagerException("Unsupported method editConfig");
    }
}