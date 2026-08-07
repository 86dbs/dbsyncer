/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer.parser;

/**
 * LogMiner UPDATE redo 中未出现的列占位（区别于显式 SQL NULL）。
 * <p>
 * 写入前须由 {@code OracleListener} 按主键回查源表覆盖，避免全字段 UPDATE/MERGE 把目标 LOB 清空。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-07 16:45
 */
public final class OracleUnchangedValue {

    public static final OracleUnchangedValue INSTANCE = new OracleUnchangedValue();

    private OracleUnchangedValue() {
    }

    public static boolean isUnchanged(Object value) {
        return value == INSTANCE;
    }

    @Override
    public String toString() {
        return "__UNCHANGED__";
    }
}
