/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.schema;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.schema.CustomData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 几何数据类型值转换
 *
 * @author 穿云
 * @version 1.0.0
 * @date 2026-01-18 13:03
 */
public class SqlServerGeometryData extends CustomData {

    public SqlServerGeometryData(Object value) {
        super(value);
    }

    @Override
    public Collection<?> apply() {
        List<Object> args = new ArrayList<>();
        // POINT (133.4 38.5) | 4326
        // SQL: CASE WHEN ? IS NULL THEN CAST(NULL AS geometry) ELSE geometry::STGeomFromText(?, ?) END
        // 参数顺序：[wkt_for_null_check, wkt_for_function, srid]
        String valueStr = (String) getValue();
        if (StringUtil.isBlank(valueStr)) {
            // 空字符串也当作 NULL 处理
            args.add(null);
            args.add(null);
            args.add(null);
            return args;
        }
        GeometryData data = parse(valueStr);
        // 第一个参数用于 NULL 检查（如果 WKT 为空或 null，则整个表达式返回 NULL）
        boolean blankWKT = StringUtil.isBlank(data.wkt);
        args.add(blankWKT ? null : data.wkt);
        args.add(blankWKT ? null : data.wkt);  // WKT 参数
        args.add(blankWKT ? null : data.srid); // SRID 参数
        return args;
    }

    /**
     * 解析包含 SRID 的 WKT 字符串
     * 格式: "SRID=4326;POINT(133.4 38.5)" 或 "POINT(133.4 38.5) | 4326"
     */
    public GeometryData parse(String input) {
        GeometryData data = new GeometryData();
        if (input == null || input.trim().isEmpty()) {
            return data;
        }

        // 移除首尾空格
        input = input.trim();

        // 处理格式1: "POINT (133.4 38.5) | 4326"
        if (input.contains("|")) {
            String[] parts = input.split("\\|");
            if (parts.length == 2) {
                data.wkt=parts[0].trim();
                try {
                    data.srid=Integer.parseInt(parts[1].trim());
                } catch (NumberFormatException e) {
                    data.srid = 0;
                }
            }
        }
        // 处理格式2: "SRID=4326;POINT(133.4 38.5)"
        else if (input.toUpperCase().startsWith("SRID=")) {
            int semicolonIndex = input.indexOf(';');
            if (semicolonIndex > 0) {
                String sridPart = input.substring(0, semicolonIndex);
                String wktPart = input.substring(semicolonIndex + 1);

                // 提取 SRID 数字
                String sridStr = sridPart.replaceAll("[^0-9]", "");
                try {
                    data.srid=Integer.parseInt(sridStr);
                } catch (NumberFormatException e) {
                    data.srid = 0;
                }

                data.wkt = wktPart.trim();
            }
        }
        // 处理格式3: 纯 WKT，无 SRID
        else {
            data.wkt = input;
            data.srid = 0;
        }
        return data;
    }

    static class GeometryData {
        String wkt;
        int srid = 0;
    }

}