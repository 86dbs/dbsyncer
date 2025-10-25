/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.postgresql.decoder;

import org.dbsyncer.sdk.config.DatabaseConfig;
import org.dbsyncer.connector.postgresql.column.PgColumnValue;
import org.dbsyncer.connector.postgresql.enums.MessageTypeEnum;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.util.PGmoney;

import java.nio.ByteBuffer;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-04-10 22:36
 */
public abstract class AbstractMessageDecoder implements MessageDecoder {

    protected String metaId;

    protected DatabaseConfig config;
    protected String database;
    protected String schema;

    @Override
    public boolean skipMessage(ByteBuffer buffer, LogSequenceNumber startLsn, LogSequenceNumber lastReceiveLsn) {
        if (null == lastReceiveLsn || lastReceiveLsn.asLong() == 0 || startLsn.equals(lastReceiveLsn)) {
            return true;
        }

        int position = buffer.position();
        try {
            MessageTypeEnum type = MessageTypeEnum.getType((char) buffer.get());
            switch (type) {
                case BEGIN:
                case COMMIT:
                case RELATION:
                case TRUNCATE:
                case TYPE:
                case ORIGIN:
                case NONE:
                    return true;
                default:
                    // TABLE|INSERT|UPDATE|DELETE
                    return false;
            }
        } finally {
            buffer.position(position);
        }
    }

    @Override
    public String getSlotName() {
        return String.format("dbs_slot_%s_%s_%s", schema, config.getUsername(), metaId).toLowerCase();
    }

    @Override
    public void setMetaId(String metaId) {
        this.metaId = metaId;
    }

    @Override
    public void setConfig(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public void setDatabase(String database) {
        this.database = database;
    }

    @Override
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * Resolve value
     *
     * @param typeName
     * @param columnValue
     * @return
     */
    protected Object resolveValue(String typeName, String columnValue) {
        PgColumnValue value = new PgColumnValue(columnValue);
        if (value.isNull()) {
            // nulls are null
            return null;
        }

        switch (typeName) {
            // include all types from https://www.postgresql.org/docs/current/static/datatype.html#DATATYPE-TABLE
            case "boolean":
            case "bool":
                return value.asBoolean();

            case "integer":
            case "int":
            case "int4":
            case "smallint":
            case "int2":
            case "smallserial":
            case "serial":
            case "serial2":
            case "serial4":
                return value.asInteger();

            case "bigint":
            case "bigserial":
            case "int8":
            case "oid":
                return value.asLong();

            case "real":
            case "float4":
                return value.asFloat();

            case "double precision":
            case "float8":
                return value.asDouble();

            case "numeric":
            case "decimal":
                return value.asBigDecimal();

            case "character":
            case "char":
            case "character varying":
            case "varchar":
            case "bpchar":
            case "text":
            case "hstore":
                return value.asString();

            case "date":
                return value.asDate();

            case "timestamp with time zone":
            case "timestamptz":
                return value.asOffsetDateTimeAtUtc();

            case "timestamp":
            case "timestamp without time zone":
                return value.asTimestamp();

            case "time":
                return value.asTime();

            case "time without time zone":
                return value.asLocalTime();

            case "time with time zone":
            case "timetz":
                return value.asOffsetTimeUtc();

            case "bytea":
                return value.asByteArray();

            // these are all PG-specific types and we use the JDBC representations
            // note that, with the exception of point, no converters for these types are implemented yet,
            // i.e. those values won't actually be propagated to the outbound message until that's the case
            case "box":
                return value.asBox();
            case "circle":
                return value.asCircle();
            case "interval":
                return value.asInterval();
            case "line":
                return value.asLine();
            case "lseg":
                return value.asLseg();
            case "money":
                final Object v = value.asMoney();
                return (v instanceof PGmoney) ? ((PGmoney) v).val : v;
            case "path":
                return value.asPath();
            case "point":
                return value.asPoint();
            case "polygon":
                return value.asPolygon();

            // PostGIS types are HexEWKB strings
            // ValueConverter turns them into the correct types
            case "geometry":
            case "geography":
            case "citext":
            case "bit":
            case "bit varying":
            case "varbit":
            case "json":
            case "jsonb":
            case "xml":
            case "uuid":
            case "tsrange":
            case "tstzrange":
            case "daterange":
            case "inet":
            case "cidr":
            case "macaddr":
            case "macaddr8":
            case "int4range":
            case "numrange":
            case "int8range":
                return value.asString();

            // catch-all for other known/builtin PG types
            case "pg_lsn":
            case "tsquery":
            case "tsvector":
            case "txid_snapshot":
                // catch-all for unknown (extension module/custom) types
            default:
                return null;
        }

    }
}