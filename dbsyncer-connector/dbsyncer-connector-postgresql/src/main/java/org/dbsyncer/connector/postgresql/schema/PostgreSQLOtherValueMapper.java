/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.schema;

import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.connector.postgresql.PostgreSQLException;
import org.dbsyncer.sdk.connector.AbstractValueMapper;
import org.dbsyncer.sdk.connector.ConnectorInstance;
import org.dbsyncer.sdk.connector.CustomType;
import org.postgis.Geometry;
import org.postgis.PGgeometry;
import org.postgis.binary.BinaryParser;
import org.postgis.binary.BinaryWriter;
import org.postgresql.geometric.PGpoint;

/**
 * JDBC索引{@link java.sql.Types 1111}, JDBC类型java.lang.Object，支持的数据库类型：
 * <ol>
 * <li>cidr</li>
 * <li>inet</li>
 * <li>macaddr</li>
 * <li>box</li>
 * <li>circle</li>
 * <li>interval</li>
 * <li>line</li>
 * <li>lseg</li>
 * <li>path</li>
 * <li>point</li>
 * <li>polygon</li>
 * <li>varbit</li>
 * </ol>
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-12-22 22:59
 */
public final class PostgreSQLOtherValueMapper extends AbstractValueMapper<CustomType> {

    @Override
    protected boolean skipConvert(Object val) {
        return val instanceof PGpoint || val instanceof PGgeometry || val instanceof Geometry || val instanceof java.util.UUID;
    }

    @Override
    protected CustomType convert(ConnectorInstance connectorInstance, Object val) {
        if (val instanceof String) {
            String s = (String) val;
            if (UUIDUtil.isUUID(s)) {
                return new CustomType(UUIDUtil.fromString(s));
            }
            BinaryParser parser = new BinaryParser();
            Geometry geo = parser.parse((String) val);
            BinaryWriter bw = new BinaryWriter();
            return new CustomType(bw.writeBinary(geo));
        }
        throw new PostgreSQLException(String.format("%s can not find type [%s], val [%s]", getClass().getSimpleName(), val.getClass(), val));
    }
}