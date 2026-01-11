/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.file.cdc;

import org.apache.commons.io.IOUtils;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.NumberUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.connector.file.FileConnector;
import org.dbsyncer.connector.file.FileConnectorInstance;
import org.dbsyncer.connector.file.FileException;
import org.dbsyncer.connector.file.model.FileResolver;
import org.dbsyncer.sdk.constant.ConnectorConstant;
import org.dbsyncer.sdk.listener.AbstractListener;
import org.dbsyncer.sdk.listener.event.RowChangedEvent;
import org.dbsyncer.sdk.model.ChangedOffset;
import org.dbsyncer.sdk.model.Field;
import org.dbsyncer.sdk.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2022-05-05 23:19
 */
public class FileListener extends AbstractListener<FileConnectorInstance> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String OFFSET = "pos_";
    private static final String CHARSET_NAME = "UTF-8";
    private final Lock connectLock = new ReentrantLock();
    private volatile boolean connected;
    private WatchService watchService;
    private Worker worker;
    private final Map<String, PipelineResolver> pipeline = new ConcurrentHashMap<>();
    private final FileResolver fileResolver = new FileResolver();
    private String fileDir;

    @Override
    public void start() {
        try {
            connectLock.lock();
            if (connected) {
                logger.error("FileExtractor is already started");
                return;
            }

            FileConnectorInstance instance = getConnectorInstance();
            fileDir = instance.getConfig().getFileDir();
            if (!StringUtil.endsWith(fileDir, File.separator)) {
                fileDir += File.separator;
            }
            connected = true;

            initPipeline();
            watchService = FileSystems.getDefault().newWatchService();
            Path p = Paths.get(fileDir);
            p.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

            worker = new Worker();
            worker.setName("file-parser-" + getConnectorInstanceCacheKey() + "_" + worker.hashCode());
            worker.setDaemon(false);
            worker.start();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            closePipelineAndWatch();
            throw new FileException(e);
        } finally {
            connectLock.unlock();
        }
    }

    public String getConnectorInstanceCacheKey() {
        String localIP;
        try {
            localIP = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.error(e.getMessage());
            localIP = "127.0.0.1";
        }
        return String.format("%s-%s", localIP, fileDir);
    }

    private void initPipeline() throws IOException {
        boolean needFlush = false;
        for (Table t : customTable) {
            String fileName = t.getName();
            String file = fileDir.concat(fileName);
            Assert.isTrue(new File(file).exists(), String.format("found not file '%s'", file));

            final RandomAccessFile raf = new BufferedRandomAccessFile(file, "r");
            final String filePosKey = getFilePosKey(fileName);
            if (snapshot.containsKey(filePosKey)) {
                raf.seek(NumberUtil.toLong(snapshot.get(filePosKey), 0L));
            } else {
                raf.seek(raf.length());
                snapshot.put(filePosKey, String.valueOf(raf.getFilePointer()));
                needFlush = true;
            }
            String separator = t.getExtInfo().getProperty(FileConnector.FILE_SEPARATOR, StringUtil.VERTICAL_LINE);
            pipeline.put(fileName, new PipelineResolver(t.getColumn(), separator.charAt(0), raf));
        }
        if (needFlush) {
            super.forceFlushEvent();
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

    @Override
    public void refreshEvent(ChangedOffset offset) {
        if (offset.getNextFileName() != null && offset.getPosition() != null) {
            snapshot.put(offset.getNextFileName(), String.valueOf(offset.getPosition()));
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
        return OFFSET.concat(fileName);
    }

    private void parseEvent(String fileName) throws IOException {
        if (pipeline.containsKey(fileName)) {
            PipelineResolver pipelineResolver = pipeline.get(fileName);
            final RandomAccessFile raf = pipelineResolver.raf;

            final String filePosKey = getFilePosKey(fileName);
            List<List> list = new ArrayList<>();
            String line;
            while (null != (line = pipelineResolver.readLine())) {
                if (StringUtil.isNotBlank(line)) {
                    list.add(fileResolver.parseList(pipelineResolver.fields, pipelineResolver.separator, line));
                }
            }

            if (!CollectionUtils.isEmpty(list)) {
                int size = list.size();
                for (int i = 0; i < size; i++) {
                    RowChangedEvent event = new RowChangedEvent(fileName, ConnectorConstant.OPERTION_INSERT, list.get(i), null, null);
                    if (i == size - 1) {
                        event.setNextFileName(filePosKey);
                        event.setPosition(raf.getFilePointer());
                    }
                    changeEvent(event);
                }
            }
        }
    }

    static final class PipelineResolver {
        List<Field> fields;
        char separator;
        RandomAccessFile raf;
        byte[] b;
        long filePointer;

        public PipelineResolver(List<Field> fields, char separator, RandomAccessFile raf) {
            this.fields = fields;
            this.separator = separator;
            this.raf = raf;
        }

        public String readLine() throws IOException {
            this.filePointer = raf.getFilePointer();
            if (filePointer >= raf.length()) {
                b = new byte[0];
                return null;
            }
            if (b == null || b.length == 0) {
                b = new byte[(int) (raf.length() - filePointer)];
            }
            raf.read(b);

            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            int read = 0;
            for (int i = 0; i < b.length; i++) {
                read++;
                if (b[i] == '\n' || b[i] == '\r') {
                    break;
                }
                stream.write(b[i]);
            }
            b = Arrays.copyOfRange(b, read, b.length);

            raf.seek(this.filePointer + read);
            byte[] _b = stream.toByteArray();
            stream.close();
            stream = null;
            return new String(_b, CHARSET_NAME);
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
                } catch (ClosedWatchServiceException e) {
                    break;
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