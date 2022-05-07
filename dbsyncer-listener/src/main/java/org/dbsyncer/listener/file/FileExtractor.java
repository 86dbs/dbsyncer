package org.dbsyncer.listener.file;

import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.RandomUtil;
import org.dbsyncer.connector.config.FileConfig;
import org.dbsyncer.connector.file.FileConnectorMapper;
import org.dbsyncer.connector.model.FileSchema;
import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.ListenerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.*;
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

    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private FileConnectorMapper connectorMapper;
    private WatchService watchService;
    private Worker worker;
    private Map<String, RandomAccessFile> pipeline = new ConcurrentHashMap<>();
    private static final byte[] buffer = new byte[4096];
    private static final String POS_PREFIX = "pos_";

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

            initPipeline(config.getFileDir(), config.getSchema());
            watchService = FileSystems.getDefault().newWatchService();
            Path p = Paths.get(config.getFileDir());
            p.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

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
            String file = fileDir.concat(fileSchema.getFileName());
            Assert.isTrue(new File(file).exists(), String.format("found not file '%s'", file));

            final RandomAccessFile raf = new RandomAccessFile(file, "r");
            pipeline.put(fileSchema.getFileName(), raf);

            final String filePosKey = getFilePosKey(fileSchema.getFileName());
            if (snapshot.containsKey(filePosKey)) {
                raf.seek(NumberUtil.toLong(snapshot.get(filePosKey), 0L));
                continue;
            }

            raf.seek(raf.length());
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
            pipeline.values().forEach(raf -> IOUtils.closeQuietly(raf));
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
            final RandomAccessFile raf = pipeline.get(fileName);

            int len = 0;
            while (-1 != len) {
                len = raf.read(buffer);
                if (0 < len) {
                    // TODO 解析 line
                    logger.info("offset:{}, len:{}", raf.getFilePointer(), len);
                    logger.info(new String(buffer, 1, len, "UTF-8"));
                    continue;
                }
            }

            final String filePosKey = getFilePosKey(fileName);
            snapshot.put(filePosKey, String.valueOf(raf.getFilePointer()));
        }
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted() && connected) {
                try {
                    WatchKey watchKey = watchService.take();
                    List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
                    for (WatchEvent<?> event : watchEvents) {
                        parseEvent(event.context().toString());
                    }
                    watchKey.reset();
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }
        }

    }

}
