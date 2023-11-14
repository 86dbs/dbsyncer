package org.dbsyncer.connector.file;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.config.FileConfig;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.FileSchema;
import org.dbsyncer.sdk.spi.ConnectorMapper;
import org.springframework.util.Assert;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/5 23:19
 */
public final class FileConnectorMapper implements ConnectorMapper<FileConfig, String> {

    private FileConfig config;
    private List<FileSchema> fileSchemaList;
    private Map<String, FileResolver> fileSchemaMap = new ConcurrentHashMap<>();

    public FileConnectorMapper(FileConfig config) {
        this.config = config;
        fileSchemaList = JsonUtil.jsonToArray(config.getSchema(), FileSchema.class);
        Assert.notEmpty(fileSchemaList, "The schema is empty.");
        for (FileSchema fileSchema : fileSchemaList) {
            final List<Field> fields = fileSchema.getFields();
            Assert.notEmpty(fields, "The fields of file schema is empty.");

            if (!fileSchemaMap.containsKey(fileSchema.getFileName())) {
                fileSchemaMap.put(fileSchema.getFileName(), new FileResolver(fileSchema));
            }
        }
    }

    public FileConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(FileConfig config) {
        this.config = config;
    }

    @Override
    public String getConnection() {
        return config.getFileDir();
    }

    @Override
    public void close() {
        fileSchemaMap.clear();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public List<FileSchema> getFileSchemaList() {
        return fileSchemaList;
    }

    public FileSchema getFileSchema(String tableName) {
        FileResolver fileResolver = fileSchemaMap.get(tableName);
        Assert.notNull(fileResolver, String.format("can not find fileSchema by tableName '%s'", tableName));
        return fileResolver.fileSchema;
    }

    public String getFilePath(String tableName) {
        FileResolver fileResolver = fileSchemaMap.get(tableName);
        Assert.notNull(fileResolver, String.format("can not find fileSchema by tableName '%s'", tableName));
        return fileResolver.filePath;
    }

    class FileResolver {
        FileSchema fileSchema;
        String filePath;

        public FileResolver(FileSchema fileSchema) {
            this.fileSchema = fileSchema;
            this.filePath = config.getFileDir().concat(fileSchema.getFileName());
            File file = new File(filePath);
            Assert.isTrue(file.exists(), String.format("found not file '%s'", filePath));
        }
    }
}
