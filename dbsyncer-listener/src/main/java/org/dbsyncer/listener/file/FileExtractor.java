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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.Arrays;
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

    private static final int BUFFER_SIZE = 55;
    private static final byte[] buffer = new byte[BUFFER_SIZE];
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

            final RandomAccessFile raf = new BufferedRandomAccessFile(file, "r");
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
            pipeline.values().forEach(pipelineResolver -> IOUtils.closeQuietly(pipelineResolver.raf));
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
            final RandomAccessFile raf = pipelineResolver.raf;
            logger.info("{}", raf.getFilePointer());

//            byte[] buffer = new byte[BUFFER_SIZE];
//            int len = 0;
//            while (-1 != len) {
//                len = raf.read(buffer);
//                if (0 < len) {
//                    AtomicInteger pointer = new AtomicInteger();
//                    AtomicInteger limit = new AtomicInteger(len);
//                    String line;
//                    while (null != (line = readLine(raf, buffer, pointer, limit))) {
//                        List<Object> row = fileResolver.parseList(pipelineResolver.fields, separator, line);
//                        changedEvent(new RowChangedEvent(fileName, ConnectorConstant.OPERTION_UPDATE, Collections.EMPTY_LIST, row));
//                    }
//                }
//            }
            final String filePosKey = getFilePosKey(fileName);
            String line;
            while (null != (line = pipelineResolver.readLine())) {
                logger.info(line);
                logger.info("{}", raf.getFilePointer());
                snapshot.put(filePosKey, String.valueOf(raf.getFilePointer()));
                List<Object> row = fileResolver.parseList(pipelineResolver.fields, separator, line);
                changedEvent(new RowChangedEvent(fileName, ConnectorConstant.OPERTION_UPDATE, Collections.EMPTY_LIST, row));
            }

        }
    }

//    public final String readLine(RandomAccessFile raf, byte[] buffer, AtomicInteger pointer, AtomicInteger limit) throws IOException {
//        String s = new String(buffer, 0, limit.get(), CHARSET_NAME);
//
//        int offset = pointer.get();
//        int end = 0;
//        boolean eol = false;
//        while (!eol && pointer.get() < limit.get()) {
//            char c = (char) buffer[pointer.get()];
//            switch (c) {
//                case '\n':
//                case '\r':
//                    eol = true;
//                    break;
//                default:
//                    end++;
//                    break;
//            }
//            pointer.getAndAdd(1);
//        }
//        if (end == 0) {
//            return null;
//        }
//
//        if (!eol) {
//            long rollback = raf.getFilePointer() - pointer.get() - offset;
//            raf.seek(rollback);
//            return null;
//        }
//        String s1 = new String(buffer, offset, end, CHARSET_NAME);
//        logger.info(s1);
//        return s1;
//    }

    final class PipelineResolver {
        List<Field> fields;
        RandomAccessFile raf;
        byte[] b;
        long filePointer;

        public PipelineResolver(List<Field> fields, RandomAccessFile raf) {
            this.fields = fields;
            this.raf = raf;
        }

        public String readLine() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int size = 0;
            boolean ifCRLF = false;
            int read = 0; // 已读数
            while (true) {
                // 填充byte[]
                fill();
                size = b.length;
                if (b.length == 0) {
                    return null;
                }
                for (int i = 0; i < size; i++) {
                    read++;
                    if (b[i] == '\n') {
                        ifCRLF = true;
                        break;
                    }
                    if (b[i] != '\r') {
                        baos.write(b[i]);
                    }
                }

                // 重置b
                b = Arrays.copyOfRange(b, read, size);
                if (ifCRLF)
                    break;
            }

            byte[] b = baos.toByteArray();
            raf.seek(this.filePointer + read);
            baos.close();
            baos = null;
            return new String(b, CHARSET_NAME);
        }

        private void fill() throws IOException {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            if (b != null && b.length > 0) {
                stream.write(b, 0, b.length);
            }
            int size = 0;
            long len = raf.length();
            long pointer = raf.getFilePointer();
            if (pointer >= len) {
                b = new byte[0];
                return;
            }
            byte[] _b = new byte[(int) (len - pointer)];
            this.filePointer = raf.getFilePointer();
            size = raf.read(_b);
            stream.write(_b, 0, size);
            _b = null;
            b = stream.toByteArray();
            stream = null;
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
