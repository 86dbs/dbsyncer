package org.dbsyncer.sdk.schema.support;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.dbsyncer.sdk.enums.DataTypeEnum;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.schema.AbstractDataType;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Geometry标准数据类型
 * 用于存储WKB（Well-Known Binary）格式的空间几何数据
 */
public abstract class GeometryType extends AbstractDataType<byte[]> {

    protected GeometryType() {
        super(byte[].class);
    }

    @Override
    public DataTypeEnum getType() {
        return DataTypeEnum.GEOMETRY;
    }

    /**
     * 重写 mergeValue 方法，确保即使 val 是 byte[] 类型，也要调用 merge 方法进行转换
     * 因为不同数据库的 Geometry 数据虽然都是 byte[] 格式，但格式不同：
     * - MySQL: [SRID(4字节)] + [WKB数据]
     * - PostgreSQL: EWKB格式（通过HexEWKB字符串或byte[]）
     * - SQL Server: 自定义二进制格式
     * - Oracle: pickle格式（通过Struct或byte[]）
     * - SQLite: 标准WKB格式（可以直接返回）
     * 
     * 如果直接返回 byte[]，会导致数据库特定的格式被误当作标准WKB格式
     */
    @Override
    public Object mergeValue(Object val, Field field) {
        if (val == null) {
            return getDefaultMergedVal();
        }
        // 对于 Geometry 类型，即使 val 是 byte[] 类型，也需要调用 merge 方法进行转换
        // 因为不同数据库的 byte[] 格式不同，需要转换为标准WKB格式
        return merge(val, field);
    }

    @Override
    protected byte[] merge(Object val, Field field) {
        if (val instanceof byte[]) {
            return (byte[]) val;
        }
        // 对于其他类型，由子类实现具体的转换逻辑
        return throwUnsupportedException(val, field);
    }

    @Override
    protected Object convert(Object val, Field field) {
        if (val instanceof byte[]) {
            return val;
        }
        // 对于其他类型，由子类实现具体的转换逻辑
        return throwUnsupportedException(val, field);
    }

    @Override
    public Field handleDDLParameters(ColDataType colDataType) {
        Field result = super.handleDDLParameters(colDataType);
        
        // 尝试从DDL参数中提取SRID
        // 不同数据库的格式可能不同：
        // - MySQL: GEOMETRY SRID 4326 或 GEOMETRY(4326)
        // - PostgreSQL: GEOMETRY(POINT, 4326) 或 GEOMETRY(4326)
        // - SQL Server: GEOMETRY (SRID在数据中，DDL中不指定)
        // - Oracle: SDO_GEOMETRY (SRID在数据中，DDL中不指定)
        // - SQLite: GEOMETRY (SRID可能通过SpatiaLite扩展指定)
        
        List<String> argsList = colDataType.getArgumentsStringList();
        if (argsList != null && !argsList.isEmpty()) {
            // 尝试从参数列表中提取SRID
            // 通常SRID是最后一个数字参数
            for (int i = argsList.size() - 1; i >= 0; i--) {
                String arg = argsList.get(i).trim();
                try {
                    int srid = Integer.parseInt(arg);
                    if (srid >= 0 && srid <= 999999) { // SRID的有效范围
                        result.setSrid(srid);
                        break;
                    }
                } catch (NumberFormatException e) {
                    // 不是数字，继续查找
                }
            }
        }
        
        // 尝试从类型名称中提取SRID（例如：GEOMETRY SRID 4326）
        String typeName = colDataType.getDataType().toUpperCase();
        Pattern sridPattern = Pattern.compile("SRID\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = sridPattern.matcher(typeName);
        if (matcher.find()) {
            try {
                int srid = Integer.parseInt(matcher.group(1));
                result.setSrid(srid);
            } catch (NumberFormatException e) {
                // 忽略解析错误
            }
        }
        
        return result;
    }
}

