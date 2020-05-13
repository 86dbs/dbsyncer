package org.dbsyncer.listener.extractor;

import org.dbsyncer.connector.config.DatabaseConfig;
import org.dbsyncer.listener.DefaultExtractor;
import org.dbsyncer.listener.mysql.binlog.BinlogEventListener;
import org.dbsyncer.listener.mysql.binlog.BinlogEventV4;
import org.dbsyncer.listener.mysql.binlog.BinlogRemoteClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 1.0.0
 * @Author AE86
 * @Date 2020-05-12 21:14
 */
public class MysqlExtractor extends DefaultExtractor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private BinlogRemoteClient client;

    @Override
    public void extract() {
        try {
            init();

            client.setBinlogEventListener(new BinlogEventListener() {

                @Override
                public void onEvents(BinlogEventV4 event) {
                }
            });
            client.start();
        } catch (Exception e) {
            logger.error("启动失败:{}", e.getMessage());
        }
    }

    @Override
    public void extractTiming() {

    }

    @Override
    public void close() {
        try {
            client.stopQuietly();
        } catch (Exception e) {
            logger.error("关闭失败:{}", e.getMessage());
        }
    }

    private void init() {
        final DatabaseConfig config = (DatabaseConfig) super.connectorConfig;
        // TODO 支持解析集群地址 jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true&useUnicode=true&amp;characterEncoding=UTF8&amp;useSSL=true
        String url = config.getUrl();
        String username = config.getUsername();
        String password = config.getPassword();
        String threadSuffixName = "";

        client = new BinlogRemoteClient("127.0.0.1", 3306, username, password, threadSuffixName);
        client.setBinlogFileName("mysql-bin.000001");
        client.setBinlogPosition(4);
    }
}