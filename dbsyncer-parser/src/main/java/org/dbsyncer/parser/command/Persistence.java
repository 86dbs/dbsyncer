/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.parser.command;

import org.dbsyncer.parser.ParserException;

/**
 * 序列化接口
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-12 01:32
 */
public interface Persistence {

    default boolean addConfig() throws Exception {
        throw new ParserException("Unsupported method addConfig");
    }

    default boolean editConfig() throws Exception {
        throw new ParserException("Unsupported method editConfig");
    }
}