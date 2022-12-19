package org.dbsyncer.connector;

import org.dbsyncer.common.spi.ConnectorMapper;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.WriterBatchConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected static final Map<Integer, ValueMapper> valueMappers = new LinkedHashMap<>();

    static {
        // 常用类型
        valueMappers.putIfAbsent(Types.VARCHAR, new VarcharValueMapper());
        valueMappers.putIfAbsent(Types.INTEGER, new IntegerValueMapper());
        valueMappers.putIfAbsent(Types.BIGINT, new BigintValueMapper());
        valueMappers.putIfAbsent(Types.TIMESTAMP, new TimestampValueMapper());
        valueMappers.putIfAbsent(Types.DATE, new DateValueMapper());

        // 较少使用
        valueMappers.putIfAbsent(Types.CHAR, new CharValueMapper());
        valueMappers.putIfAbsent(Types.NCHAR, new NCharValueMapper());
        valueMappers.putIfAbsent(Types.NVARCHAR, new NVarcharValueMapper());
        valueMappers.putIfAbsent(Types.LONGVARCHAR, new LongVarcharValueMapper());
        valueMappers.putIfAbsent(Types.NUMERIC, new NumberValueMapper());
        valueMappers.putIfAbsent(Types.BINARY, new BinaryValueMapper());

        // 很少使用
        valueMappers.putIfAbsent(Types.SMALLINT, new SmallintValueMapper());
        valueMappers.putIfAbsent(Types.TINYINT, new TinyintValueMapper());
        valueMappers.putIfAbsent(Types.TIME, new TimeValueMapper());
        valueMappers.putIfAbsent(Types.DECIMAL, new DecimalValueMapper());
        valueMappers.putIfAbsent(Types.DOUBLE, new DoubleValueMapper());
        valueMappers.putIfAbsent(Types.FLOAT, new FloatValueMapper());
        valueMappers.putIfAbsent(Types.BIT, new BitValueMapper());
        valueMappers.putIfAbsent(Types.BLOB, new BlobValueMapper());
        valueMappers.putIfAbsent(Types.CLOB, new ClobValueMapper());
        valueMappers.putIfAbsent(Types.NCLOB, new NClobValueMapper());
        valueMappers.putIfAbsent(Types.ROWID, new RowIdValueMapper());
        valueMappers.putIfAbsent(Types.REAL, new RealValueMapper());
        valueMappers.putIfAbsent(Types.VARBINARY, new VarBinaryValueMapper());
        valueMappers.putIfAbsent(Types.LONGVARBINARY, new LongVarBinaryValueMapper());
        valueMappers.putIfAbsent(Types.OTHER, new OtherValueMapper());
    }

    /**
     * 转换字段值
     *
     * @param connectorMapper
     * @param config
     */
    protected void convertProcessBeforeWriter(ConnectorMapper connectorMapper, WriterBatchConfig config) {
        if (CollectionUtils.isEmpty(config.getFields()) || CollectionUtils.isEmpty(config.getData())) {
            return;
        }

        // 获取字段映射规则
        for (Map row : config.getData()) {
            // 根据目标字段类型转换值
            for (Field f : config.getFields()) {
                if(null == f){
                    continue;
                }
                // 根据字段类型转换值
                final ValueMapper valueMapper = valueMappers.get(f.getType());
                if (null != valueMapper) {
                    // 当数据类型不同时，转换值类型
                    try {
                        row.put(f.getName(), valueMapper.convertValue(connectorMapper, row.get(f.getName())));
                    } catch (Exception e) {
                        logger.error("convert value error: ({}, {})", f.getName(), row.get(f.getName()));
                        throw new ConnectorException(e);
                    }
                }
            }
        }
    }

    protected Field getPrimaryKeyField(List<Field> fields) {
        for (Field f : fields) {
            if (f.isPk()) {
                return f;
            }
        }
        throw new ConnectorException("主键为空");
    }

    protected boolean isUpdate(String event) {
        return StringUtil.equals(ConnectorConstant.OPERTION_UPDATE, event);
    }

    protected boolean isInsert(String event) {
        return StringUtil.equals(ConnectorConstant.OPERTION_INSERT, event);
    }

    protected boolean isDelete(String event) {
        return StringUtil.equals(ConnectorConstant.OPERTION_DELETE, event);
    }
}