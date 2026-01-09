/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file.validator;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.file.FileConnector;
import org.dbsyncer.connector.file.config.FileConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.springframework.util.Assert;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * 文件连接配置校验器实现
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-06 00:04
 */
public class FileConfigValidator implements ConfigValidator<FileConnector, FileConfig> {

    @Override
    public void modify(FileConnector connectorService, FileConfig fileConfig, Map<String, String> params) {
        String fileDir = params.get("fileDir");
        Assert.hasText(fileDir, "fileDir is empty.");
        Assert.isTrue(new File(fileDir).exists(), String.format("路径无效 '%s'", fileDir));
        fileConfig.setFileDir(fileDir);
    }

    @Override
    public Table modifyExtendedTable(FileConnector connectorService, Map<String, String> params) {
        Table table = new Table();
        String tableName = params.get("tableName");
        String columnList = params.get("columnList");
        Assert.hasText(tableName, "TableName is empty");
        Assert.hasText(columnList, "ColumnList is empty");
        List<Field> fields = JsonUtil.jsonToArray(columnList, Field.class);
        Assert.notEmpty(fields, "字段不能为空.");
        table.setName(tableName);
        table.setColumn(fields);
        table.setType(connectorService.getExtendedTableType().getCode());
        return table;
    }

}