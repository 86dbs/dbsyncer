package org.dbsyncer.listener.mysql;

import com.github.shyiko.mysql.binlog.event.Event;
import org.apache.commons.lang.StringUtils;
import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.listener.AbstractExtractor;
import org.dbsyncer.listener.ListenerException;
import org.dbsyncer.listener.config.Host;
import org.dbsyncer.listener.mysql.binlog.impl.event.AbstractBinlogEventV4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static java.util.regex.Pattern.compile;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-12-01 21:14
 */
public class NewExtractor extends AbstractExtractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String          BINLOG_FILENAME = "fileName";
    private static final String          BINLOG_POSITION = "position";
    private static final int             RETRY_TIMES     = 3;
    private static final int             MASTER          = 0;
    private              BinaryLogClient client;
    private              List<Host>      cluster;

    @Override
    public void start() {
        try {
            run();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
            throw new ListenerException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (null != client) {
                client.disconnect();
            }
        } catch (Exception e) {
            logger.error("关闭失败:{}", e.getMessage());
        }

    }

    private void run() throws Exception {
        final DatabaseConfig config = (DatabaseConfig) connectorConfig;
        cluster = readNodes(config.getUrl());
        Assert.notEmpty(cluster, "Mysql连接地址有误.");

        final Host host = cluster.get(MASTER);
        final String username = config.getUsername();
        final String password = config.getPassword();
        final String pos = map.get(BINLOG_POSITION);
        client = new BinaryLogRemoteClient(host.getIp(), host.getPort(), username, password);
        client.setBinlogFilename(map.get(BINLOG_FILENAME));
        client.setBinlogPosition(StringUtils.isBlank(pos) ? 0 : Long.parseLong(pos));
        client.registerEventListener(new MysqlEventListener());
        client.registerLifecycleListener(new MysqlLifecycleListener());
        client.registerLifecycleListener(new MysqlLifecycleListener());

        client.connect();
    }

    private List<Host> readNodes(String url) {
        if (StringUtils.isBlank(url)) {
            return Collections.EMPTY_LIST;
        }
        Matcher matcher = compile("(//)(?!(/)).+?(/)").matcher(url);
        while (matcher.find()) {
            url = matcher.group(0);
            break;
        }
        url = StringUtils.replace(url, "/", "");

        List<Host> cluster = new ArrayList<>();
        String[] arr = StringUtils.split(url, ",");
        int size = arr.length;
        for (int i = 0; i < size; i++) {
            String[] host = StringUtils.split(arr[i], ":");
            if (2 == host.length) {
                cluster.add(new Host(host[0], Integer.parseInt(host[1])));
            }
        }
        return cluster;
    }

    private void reStart() {
        for (int i = 1; i <= RETRY_TIMES; i++) {
            try {
                if (null != client) {
                    client.disconnect();
                }
                run();

                errorEvent(new ListenerException(String.format("重启成功, %s", client.getWorkerThreadName())));
                logger.error("第{}次重启成功, ThreadName:{} ", i, client.getWorkerThreadName());
                break;
            } catch (Exception e) {
                logger.error("第{}次重启异常, ThreadName:{}, {}", i, client.getWorkerThreadName(), e.getMessage());
                // 无法连接，关闭任务
                if (i == RETRY_TIMES) {
                    interruptException(new ListenerException(String.format("重启异常, %s, %s", client.getWorkerThreadName(), e.getMessage())));
                }
            }
            try {
                TimeUnit.SECONDS.sleep(i * 2);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private void refresh(AbstractBinlogEventV4 event) {
        String binlogFilename = event.getBinlogFilename();
        long nextPosition = event.getHeader().getNextPosition();

        // binlogFileName
        if (StringUtils.isNotBlank(binlogFilename) && !StringUtils.equals(binlogFilename, client.getBinlogFilename())) {
            client.setBinlogFilename(binlogFilename);
        }
        client.setBinlogPosition(nextPosition);

        // nextPosition
        map.put(BINLOG_FILENAME, event.getBinlogFilename());
        map.put(BINLOG_POSITION, String.valueOf(nextPosition));
    }

    final class MysqlLifecycleListener implements BinaryLogRemoteClient.LifecycleListener {

        @Override
        public void onConnect(BinaryLogRemoteClient client) {
            logger.info("建立连接");
        }

        @Override
        public void onCommunicationFailure(BinaryLogRemoteClient client, Exception ex) {
            logger.error("连接异常", ex);
            // retry connect
            reStart();
        }

        @Override
        public void onEventDeserializationFailure(BinaryLogRemoteClient client, Exception ex) {
            logger.error("解析异常", ex);
        }

        @Override
        public void onDisconnect(BinaryLogRemoteClient client) {
            logger.error("断开连接");
        }

    }

    final class MysqlEventListener implements BinaryLogRemoteClient.EventListener {

        private Map<Long, String> table = new HashMap<>();

        @Override
        public void onEvent(Event event) {
            logger.info(event.toString());
        }

    }

}