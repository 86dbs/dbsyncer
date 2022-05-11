package org.dbsyncer.connector.file;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.ConnectorMapper;
import org.dbsyncer.connector.config.FileConfig;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/5 23:19
 */
public final class FileConnectorMapper implements ConnectorMapper<FileConfig, String> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private FileConfig config;
    private List<FileSchema> fileSchemaList;
    private Map<String, FileResolver> fileSchemaMap = new ConcurrentHashMap<>();

    public FileConnectorMapper(FileConfig config) throws FileNotFoundException {
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
    public String getConnection() {
        return config.getFileDir();
    }

    @Override
    public void close() {
        fileSchemaMap.values().forEach(fileResolver -> fileResolver.close());
    }

    public List<FileSchema> getFileSchemaList() {
        return fileSchemaList;
    }

    public FileSchema getFileSchema(String tableName) {
        FileResolver fileResolver = fileSchemaMap.get(tableName);
        Assert.notNull(fileResolver, String.format("can not find fileSchema by tableName '%s'", tableName));
        return fileResolver.fileSchema;
    }

    public FileChannel getFileChannel(String tableName) {
        FileResolver fileResolver = fileSchemaMap.get(tableName);
        Assert.notNull(fileResolver, String.format("can not find fileSchema by tableName '%s'", tableName));
        return fileResolver.raf.getChannel();
    }

    public String getFilePath(String tableName) {
        FileResolver fileResolver = fileSchemaMap.get(tableName);
        Assert.notNull(fileResolver, String.format("can not find fileSchema by tableName '%s'", tableName));
        return fileResolver.filePath;
    }

    class FileResolver {

        FileSchema fileSchema;
        RandomAccessFile raf;
        String filePath;

        public FileResolver(FileSchema fileSchema) throws FileNotFoundException {
            this.fileSchema = fileSchema;
            this.filePath = config.getFileDir().concat(fileSchema.getFileName());
            File file = new File(filePath);
            Assert.isTrue(file.exists(), String.format("found not file '%s'", filePath));
            this.raf = new RandomAccessFile(file, "rw");
        }

        public void close(){
            if(null != raf){
                try {
                    raf.close();
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
        }

    }
}