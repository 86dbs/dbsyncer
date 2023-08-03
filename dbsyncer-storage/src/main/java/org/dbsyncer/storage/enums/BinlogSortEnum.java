package org.dbsyncer.storage.enums;

/**
 * 支持的排序方式
 *
 * @author AE86
 * @version 1.0.0
 * @date 2023/6/1 00:07
 */
public enum BinlogSortEnum {
    /**
     * 升序
     */
    ASC("asc"),

    /**
     * 降序
     */
    DESC("desc");

    BinlogSortEnum(String code) {
        this.code = code;
    }

    String code;

    public String getCode() {
        return code;
    }

    /**
     * 是否降序
     *
     * @return
     */
    public boolean isDesc() {
        return this == DESC;
    }
}