package org.dbsyncer.sdk.schema;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;

import java.util.Set;

public interface DataType {

    /**
     * 获取支持的转换类型
     *
     * @return
     */
    Set<String> getSupportedTypeName();

    /**
     * 转换为标准数据类型
     *
     * @param val   转换值
     * @param field 数据类型
     * @return Object
     */
    Object mergeValue(Object val, Field field);

    /**
     * 转换为指定数据类型
     *
     * @param val   转换值
     * @param field 数据类型
     * @return Object
     */
    Object convertValue(Object val, Field field);

    /**
     * 获取数据类型
     *
     * @return
     */
    DataTypeEnum getType();

    /**
     * 处理DDL解析的参数，提取字段的尺寸、精度等信息
     * 默认实现不处理任何参数，具体的DataType实现类可以覆盖此方法来处理特定的DDL参数
     *
     * @param colDataType DDL解析的列数据类型对象
     * @return 包含完整信息的字段对象
     */
    default Field handleDDLParameters(ColDataType colDataType) {
        // 默认实现从colDataType中提取基本信息
        Field result = new Field();
        result.setTypeName(colDataType.getDataType());
        return result;
    }
}