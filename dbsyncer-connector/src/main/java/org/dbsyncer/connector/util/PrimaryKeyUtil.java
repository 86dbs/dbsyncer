package org.dbsyncer.connector.util;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.ConnectorException;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.Table;

import java.util.List;

public abstract class PrimaryKeyUtil {

    /**
     * 返回主键名称
     *
     * @param table
     * @return
     */
    public static String findOriginalTablePrimaryKey(Table table) {
        if (null == table) {
            return null;
        }

        // 获取自定义主键
        String pk = table.getPrimaryKey();
        if (StringUtil.isNotBlank(pk)) {
            return pk;
        }

        // 获取表原始主键
        List<Field> column = table.getColumn();
        if (!CollectionUtils.isEmpty(column)) {
            for (Field c : column) {
                if (c.isPk()) {
                    return c.getName();
                }
            }
        }

        throw new ConnectorException(String.format("The primary key of table '%s' is null.", table.getName()));
    }

}