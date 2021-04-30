package org.dbsyncer.biz.enums;

/**
 * Oralce参数配置
 *
 * @author AE86
 * @version 1.0.0
 * @date 2021/4/28 23:21
 */
public enum OracleIncrementEnum {

    /**
     * ROWIDTOCHAR(ROWID) 列名
     */
    ROW_ID("ROWIDTOCHAR(ROWID)"),
    /**
     * ORACLE_ROW_ID 别名
     */
    ROW_ID_LABEL_NAME("ORACLE_ROW_ID");

    private String name;

    OracleIncrementEnum(String name) {
        this.name = name;
    }

    /**
     * 是否ROWIDTOCHAR(ROWID) 列名
     *
     * @param name
     * @return
     */
    public static boolean isRowId(String name) {
        return ROW_ID.getName().equals(name);
    }

    public String getName() {
        return name;
    }

}