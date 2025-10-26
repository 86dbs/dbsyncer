/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file.validator;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.file.FileConnector;
import org.dbsyncer.connector.file.model.FileSchema;
import org.dbsyncer.connector.file.config.FileConfig;
import org.dbsyncer.sdk.connector.ConfigValidator;
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
        String schema = params.get("schema");
        String separator = StringUtil.trim(params.get("separator"));
        Assert.hasText(fileDir, "fileDir is empty.");
        Assert.hasText(schema, "schema is empty.");
        Assert.hasText(separator, "separator is empty.");

        List<FileSchema> fileSchemas = JsonUtil.jsonToArray(schema, FileSchema.class);
        Assert.notEmpty(fileSchemas, "found not file schema.");

        fileDir += !StringUtil.endsWith(fileDir, File.separator) ? File.separator : "";
        for (FileSchema fileSchema : fileSchemas) {
            String file = fileDir.concat(fileSchema.getFileName());
            Assert.isTrue(new File(file).exists(), String.format("found not file '%s'", file));
        }

        fileConfig.setFileDir(fileDir);
        fileConfig.setSeparator(separator.charAt(0));
        fileConfig.setSchema(schema);
    }

}
