package org.dbsyncer.listener.file;

import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.event.RowChangedEvent;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.RandomUtil;
import org.dbsyncer.connector.config.FileConfig;
import org.dbsyncer.connector.constant.ConnectorConstant;
import org.dbsyncer.connector.file.FileConnectorMapper;
import org.dbsyncer.connector.file.FileResolver;
import org.dbsyncer.connector.model.Field;
import org.dbsyncer.connector.model.FileSchema;
import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.ListenerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2022/5/6 21:42
 */
public class FileExtractor extends AbstractExtractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final byte[] buffer = new byte[4096];
    private static final String POS_PREFIX = "pos_";
    private static final String CHARSET_NAME = "UTF-8";
    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private FileConnectorMapper connectorMapper;
    private WatchService watchService;
    private Worker worker;
    private Map<String, PipelineResolver> pipeline = new ConcurrentHashMap<>();
    private final FileResolver fileResolver = new FileResolver();
    private char separator;

    @Override
    public void start() {
        try {
            connectLock.lock();
            if (connected) {
                logger.error("FileExtractor is already started");
                return;
            }

            connectorMapper = (FileConnectorMapper) connectorFactory.connect(connectorConfig);
            final FileConfig config = connectorMapper.getConfig();
            final String mapperCacheKey = connectorFactory.getConnector(connectorMapper).getConnectorMapperCacheKey(connectorConfig);
            connected = true;

            separator = config.getSeparator();
            initPipeline(config.getFileDir(), config.getSchema());
            watchService = FileSystems.getDefault().newWatchService();
            Path p = Paths.get(config.getFileDir());
            p.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

            for (String fileName : pipeline.keySet()) {
                parseEvent(fileName);
            }
            forceFlushEvent();

            worker = new Worker();
            worker.setName(new StringBuilder("file-parser-").append(mapperCacheKey).append("_").append(RandomUtil.nextInt(1, 100)).toString());
            worker.setDaemon(false);
            worker.start();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            closePipelineAndWatch();
            throw new ListenerException(e);
        } finally {
            connectLock.unlock();
        }
    }

    private void initPipeline(String fileDir, String schema) throws IOException {
        List<FileSchema> fileSchemas = JsonUtil.jsonToArray(schema, FileSchema.class);
        Assert.notEmpty(fileSchemas, "found not file schema.");
        for (FileSchema fileSchema : fileSchemas) {
            String fileName = fileSchema.getFileName();
            String file = fileDir.concat(fileName);
            Assert.isTrue(new File(file).exists(), String.format("found not file '%s'", file));

            final BufferedRandomAccessFile raf = new BufferedRandomAccessFile(file, "r");
            final String filePosKey = getFilePosKey(fileName);
            if (snapshot.containsKey(filePosKey)) {
                raf.seek(NumberUtil.toLong(snapshot.get(filePosKey), 0L));
            } else {
                raf.seek(raf.length());
            }

            pipeline.put(fileName, new PipelineResolver(fileSchema.getFields(), raf));
        }
    }

    @Override
    public void close() {
        try {
            closePipelineAndWatch();
            connected = false;
            if (null != worker && !worker.isInterrupted()) {
                worker.interrupt();
                worker = null;
            }
        } catch (Exception e) {
            logger.error("关闭失败:{}", e.getMessage());
        }
    }

    private void closePipelineAndWatch() {
        try {
            pipeline.values().forEach(pipelineResolver -> IOUtils.closeQuietly(pipelineResolver.getRaf()));
            pipeline.clear();

            if (null != watchService) {
                watchService.close();
            }
        } catch (IOException ex) {
            logger.error(ex.getMessage());
        }
    }

    private String getFilePosKey(String fileName) {
        return POS_PREFIX.concat(fileName);
    }

    private void parseEvent(String fileName) throws IOException {
        if (pipeline.containsKey(fileName)) {
            PipelineResolver pipelineResolver = pipeline.get(fileName);
            final BufferedRandomAccessFile raf = pipelineResolver.getRaf();

            int len = 0;
            while (-1 != len) {
                // TODO 多行出现粘包，需手动readline
                len = raf.read(buffer);
                if (0 < len) {
                    String lines = new String(buffer, 0, len, CHARSET_NAME);
                    List<Object> row = fileResolver.parseList(pipelineResolver.getFields(), separator, lines);
                    changedEvent(new RowChangedEvent(fileName, ConnectorConstant.OPERTION_UPDATE, Collections.EMPTY_LIST, row));
                }
            }

            final String filePosKey = getFilePosKey(fileName);
            snapshot.put(filePosKey, String.valueOf(raf.getFilePointer()));
        }
    }

    final class PipelineResolver {

        List<Field> fields;
        BufferedRandomAccessFile raf;

        public PipelineResolver(List<Field> fields, BufferedRandomAccessFile raf) {
            this.fields = fields;
            this.raf = raf;
        }

        public List<Field> getFields() {
            return fields;
        }

        public BufferedRandomAccessFile getRaf() {
            return raf;
        }
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted() && connected) {
                WatchKey watchKey = null;
                try {
                    watchKey = watchService.take();
                    List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
                    for (WatchEvent<?> event : watchEvents) {
                        parseEvent(event.context().toString());
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage());
                } finally {
                    if (null != watchKey) {
                        watchKey.reset();
                    }
                }
            }
        }

    }

}
