/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.service;

/**
 * @author wuji
 * @version 1.0.0
 * @date 2026-09-23 21:16
 */
public interface FullIncrementService {

    void prepareFullPhase(String taskId);

    void switchToIncrement(String string);
}
