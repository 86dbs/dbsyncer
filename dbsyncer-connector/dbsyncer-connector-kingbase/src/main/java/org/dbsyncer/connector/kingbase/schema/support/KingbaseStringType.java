/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.kingbase.schema.support;

import com.kingbase8.geometric.KBpoint;
import com.kingbase8.jdbc.KbArray;
import com.kingbase8.util.KBobject;
import org.dbsyncer.connector.postgresql.schema.PostgreSQLSchemaResolver;
import org.dbsyncer.connector.postgresql.schema.support.PostgreSQLStringType;
import org.dbsyncer.sdk.model.Field;
import org.postgresql.util.PGobject;

import java.sql.SQLException;

/**
 * @author 穿云
 * @version 1.0.0
 * @date 2026-06-06 18:30
 */
public final class KingbaseStringType extends PostgreSQLStringType {

    @Override
    protected String merge(Object val, Field field) {
        if (val instanceof KBobject) {
            return ((KBobject) val).getValue();
        }
        if (val instanceof KBpoint) {
            KBpoint point = (KBpoint) val;
            return String.format("POINT (%f %f)", point.x, point.y);
        }
        if (val instanceof KbArray) {
            return val.toString();
        }
        return super.merge(val, field);
    }

    @Override
    public Object getDefaultConvertedVal(Field field) {
        try {
            KBobject kbObject = new KBobject();
            kbObject.setType(PostgreSQLSchemaResolver.normalizeTypeName(field.getTypeName()));
            kbObject.setValue(null);
            return kbObject;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Object convert(Object val, Field field) {
        Object result = super.convert(val, field);
        if (result instanceof PGobject) {
            try {
                PGobject pgObject = (PGobject) result;
                KBobject kbObject = new KBobject();
                kbObject.setType(pgObject.getType());
                kbObject.setValue(pgObject.getValue());
                return kbObject;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }
}
