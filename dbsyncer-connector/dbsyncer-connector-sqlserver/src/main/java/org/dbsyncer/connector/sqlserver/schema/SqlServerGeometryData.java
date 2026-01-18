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
        // SQL: CASE WHEN NULLIF(?, '') IS NULL THEN CAST(NULL AS geometry) ELSE geometry::STGeomFromText(?, ?) END
        // 参数顺序：[wkt_for_check, wkt_for_function, srid]
        // 注意：SRID 不能为 NULL，所以当 WKT 为 NULL 时，必须在 CASE 表达式中处理
        Object value = getValue();
        if (value == null) {
            // NULL 值处理：第一个参数用于 NULLIF 检查，第二个和第三个用于函数（但不会被调用）
            args.add(null);  // 第一个 WKT（用于 NULLIF(?, '') 检查）
            args.add(null);  // 第二个 WKT（用于函数调用，但不会执行）
            args.add(0);     // SRID（默认值，不会执行）
            return args;
        }
        
        String valueStr = value instanceof String ? (String) value : String.valueOf(value);
        GeometryData data = parse(valueStr);
        if (StringUtil.isBlank(data.wkt)) {
            // WKT 为空，传入空字符串让 NULLIF 处理
            args.add("");
            args.add(null);
            args.add(0);
        } else {
            // WKT 有效
            args.add(data.wkt);  // 第一个 WKT（用于 NULLIF 检查）
            args.add(data.wkt);  // 第二个 WKT（用于函数调用）
            args.add(data.srid); // SRID
        }
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