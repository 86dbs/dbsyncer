package org.dbsyncer.connector;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.config.WriterBatchConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.schema.VarcharValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static Map<Integer, ValueMapper> values = new LinkedHashMap<>();

    static {
        values.putIfAbsent(Types.VARCHAR, new VarcharValueMapper());
    }

    /**
     * 获取值转换配置
     *
     * @return
     */
    protected Map<Integer, ValueMapper> getValueMapper(){
        return values;
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
        final Map<Integer, ValueMapper> mappers = getValueMapper();
        config.getData().forEach(row -> {
            // 根据目标字段类型转换值
            config.getFields().forEach(f -> {
                // 根据字段类型转换值
                ValueMapper valueMapper = mappers.get(f.getType());
                if (null == valueMapper) {
                    logger.warn("Unsupported type [{}], val [{}]", f.getType(), row.get(f.getName()));
                    return;
                }

                // 当数据类型不同时，转换值类型
                row.put(f.getName(), valueMapper.convertValue(connectorMapper, row.get(f.getName())));
            });
        });
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