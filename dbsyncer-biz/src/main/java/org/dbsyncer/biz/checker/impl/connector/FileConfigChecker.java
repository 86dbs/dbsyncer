package org.dbsyncer.biz.checker.impl.connector;

import org.dbsyncer.biz.checker.ConnectorConfigChecker;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.FileConfig;
import org.dbsyncer.connector.model.FileSchema;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/6 0:04
 */
@Component
public class FileConfigChecker implements ConnectorConfigChecker<FileConfig> {

    @Override
    public void modify(FileConfig fileConfig, Map<String, String> params) {
        String fileDir = params.get("fileDir");
        String schema = params.get("schema");
        Assert.hasText(fileDir, "fileDir is empty.");
        Assert.hasText(schema, "schema is empty.");

        fileConfig.setFileDir(fileDir);
        fileConfig.setFileSchema(JsonUtil.jsonToArray(schema, FileSchema.class));
    }

}
