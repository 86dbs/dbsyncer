/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.sdk.constant;

/**
 * Agent 采集增量位点在同步任务 {@code meta.snapshot} 中的键。
 * <p>与 LogMiner 的 {@code position}(SCN) 分开存储，互不换用。</p>
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-17
 */
public final class AgentOffsetConstant {

    public static final String REDO_SCN = "redo.scn";
    public static final String REDO_SEQUENCE = "redo.sequence";
    public static final String REDO_BLOCK = "redo.block";
    public static final String REDO_OFFSET = "redo.offset";
    public static final String REDO_PATH = "redo.path";

    private AgentOffsetConstant() {
    }
}
