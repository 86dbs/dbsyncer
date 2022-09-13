package org.dbsyncer.connector;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.WriterBatchConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.schema.*;

import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractConnector {

    private static Map<Integer, ValueMapper> values = new LinkedHashMap<>();

    static {
        // 常用类型
        values.putIfAbsent(Types.VARCHAR, new VarcharValueMapper());
        values.putIfAbsent(Types.INTEGER, new IntegerValueMapper());
        values.putIfAbsent(Types.BIGINT, new BigintValueMapper());
        values.putIfAbsent(Types.TIMESTAMP, new TimestampValueMapper());
        values.putIfAbsent(Types.DATE, new DateValueMapper());

        // 较少使用
        values.putIfAbsent(Types.CHAR, new CharValueMapper());
        values.putIfAbsent(Types.NCHAR, new NCharValueMapper());
        values.putIfAbsent(Types.NVARCHAR, new NVarcharValueMapper());
        values.putIfAbsent(Types.LONGVARCHAR, new LongVarcharValueMapper());
        values.putIfAbsent(Types.NUMERIC, new NumberValueMapper());
        values.putIfAbsent(Types.BINARY, new BinaryValueMapper());

        // 很少使用
        values.putIfAbsent(Types.SMALLINT, new SmallintValueMapper());
        values.putIfAbsent(Types.TINYINT, new TinyintValueMapper());
        values.putIfAbsent(Types.TIME, new TimeValueMapper());
        values.putIfAbsent(Types.DECIMAL, new DecimalValueMapper());
        values.putIfAbsent(Types.DOUBLE, new DoubleValueMapper());
        values.putIfAbsent(Types.FLOAT, new FloatValueMapper());
        values.putIfAbsent(Types.BIT, new BitValueMapper());
        values.putIfAbsent(Types.BLOB, new BlobValueMapper());
        values.putIfAbsent(Types.CLOB, new ClobValueMapper());
        values.putIfAbsent(Types.NCLOB, new NClobValueMapper());
        values.putIfAbsent(Types.ROWID, new RowIdValueMapper());
        values.putIfAbsent(Types.REAL, new RealValueMapper());
        values.putIfAbsent(Types.VARBINARY, new VarBinaryValueMapper());
        values.putIfAbsent(Types.LONGVARBINARY, new LongVarBinaryValueMapper());
    }

    /**
     * 获取值转换配置
     *
     * @return
     */
    protected Map<Integer, ValueMapper> getValueMapper() {
        return values;
    }

    /**
     * 转换字段值
     *
     * @param connectorMapper
     * @param config
     */
    protected void convertProcessBeforeWriter(ConnectorMapper connectorMapper, WriterBatchConfig config) throws Exception {
        if (CollectionUtils.isEmpty(config.getFields()) || CollectionUtils.isEmpty(config.getData())) {
            return;
        }

        // 获取字段映射规则
        final Map<Integer, ValueMapper> mappers = getValueMapper();
        for (Map row : config.getData()) {
            // 根据目标字段类型转换值
            for (Field f : config.getFields()) {
                // 根据字段类型转换值
                final ValueMapper valueMapper = mappers.get(f.getType());
                if (null != valueMapper) {
                    // 当数据类型不同时，转换值类型
                    row.put(f.getName(), valueMapper.convertValue(connectorMapper, row.get(f.getName())));
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