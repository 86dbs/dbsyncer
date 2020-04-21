package org.dbsyncer.parser.enums;

import org.apache.commons.lang.StringUtils;
import org.dbsyncer.parser.ParserException;

/**
 * 驱动同步方式枚举
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/21 16:19
 */
public enum ModelEnum {

    /**
     * 全量
     */
    FULL("full", "全量"),
    /**
     * 增量
     */
    INCREMENT("INCREMENT", "增量");

    private String code;
    private String message;

    ModelEnum(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public static ModelEnum getModelEnum(String code) throws ParserException {
        for (ModelEnum e : ModelEnum.values()) {
            if (StringUtils.equals(code, e.getCode())) {
                return e;
            }
        }
        throw new ParserException(String.format("Model code \"%s\" does not exist.", code));
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
