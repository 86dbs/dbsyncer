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
    NCHAR(Types.NCHAR, new NCharSetter()),
    LONGVARCHAR(Types.LONGVARCHAR, new LongVarcharSetter()),
    NUMERIC(Types.NUMERIC, new NumericSetter()),

    // 很少使用
    SMALLINT(Types.SMALLINT, new SmallintSetter()),
    TINYINT(Types.TINYINT, new TinyintSetter()),
    DECIMAL(Types.DECIMAL, new DecimalSetter()),
    DOUBLE(Types.DOUBLE, new DoubleSetter()),
    FLOAT(Types.FLOAT, new FloatSetter()),
    BLOB(Types.BLOB, new BlobSetter()),
    CLOB(Types.CLOB, new ClobSetter()),
    ROWID(Types.ROWID, new RowIdSetter()),
    REAL(Types.REAL, new RealSetter());

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
        throw new ConnectorException(String.format("Setter type \"%s\" is not supported.", type));
    }

    public int getType() {
        return type;
    }

    public Setter getSetter() {
        return setter;
    }
}