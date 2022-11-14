package org.dbsyncer.manager;

public interface CommandExecutor {

    Object execute(Command cmd);
}