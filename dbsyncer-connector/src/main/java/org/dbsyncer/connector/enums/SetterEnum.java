package org.dbsyncer.connector.enums;

import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.database.Setter;
import org.dbsyncer.connector.database.setter.*;

import java.sql.Types;

/**
 * 根据列类型设值
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/15 15:13
 */
public enum SetterEnum {

    // 常用类型(靠前，减少查找次数)
    VARCHAR(Types.VARCHAR, new VarcharSetter()),
    INTEGER(Types.INTEGER, new IntegerSetter()),
    BIGINT(Types.BIGINT, new BigintSetter()),
    TIMESTAMP(Types.TIMESTAMP, new TimestampSetter()),
    DATE(Types.DATE, new DateSetter()),

    // 较少使用
    CHAR(Types.CHAR, new CharSetter()),
    LONGVARCHAR(Types.LONGVARCHAR, new LongVarcharSetter()),
    NUMERIC(Types.NUMERIC, new NumericSetter()),

    // 很少使用
    TINYINT(Types.TINYINT, new TinyintSetter()),
    DOUBLE(Types.DOUBLE, new DoubleSetter()),
    FLOAT(Types.FLOAT, new FloatSetter());

    private int type;

    private Setter setter;

    SetterEnum(int type, Setter setter) {
        this.type = type;
        this.setter = setter;
    }

    public static Setter getSetter(int type) throws ConnectorException {
        for (SetterEnum e : SetterEnum.values()) {
            if (e.getType() == type) {
                return e.getSetter();
            }
        }
        throw new ConnectorException(String.format("Setter type \"%s\" does not exist.", type));
    }

    public int getType() {
        return type;
    }

    public Setter getSetter() {
        return setter;
    }
}