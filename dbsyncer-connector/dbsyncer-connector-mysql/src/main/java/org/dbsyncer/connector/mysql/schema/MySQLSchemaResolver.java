/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.connector.mysql.schema;

import org.dbsyncer.connector.mysql.schema.support.MySQLStringType;
import org.dbsyncer.sdk.schema.AbstractSchemaResolver;
import org.dbsyncer.sdk.schema.DataType;

import java.util.Map;

/**
 * MySQL标准数据类型解析器
 * <p>https://gitee.com/ghi/dbsyncer/wikis/%E9%A1%B9%E7%9B%AE%E8%AE%BE%E8%AE%A1/%E6%A0%87%E5%87%86%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B/MySQL</p>
 *
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2024-11-25 22:08
 */
public final class MySQLSchemaResolver extends AbstractSchemaResolver {

    @Override
    protected void initDataTypes(Map<String, DataType> map) {
        map.put("VARCHAR", new MySQLStringType());
        // TODO

    }

}