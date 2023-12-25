/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.command;

/**
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-12 01:32
 */
public interface CommandExecutor {

    Object execute(Command cmd);
}