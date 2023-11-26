/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.sqlserver.cdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @Author Xinpeng.Fu
 * @Version 1.0.0
 * @Date 2022-05-22 22:56
 */
public class LsnPuller {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 间隔拉取最新LSN时间（毫秒）
     */
    private static final long DEFAULT_POLL_INTERVAL_MILLIS = 100;
    private static volatile LsnPuller instance = null;
    private final Map<String, SqlServerListener> map = new ConcurrentHashMap<>();
    private Worker worker;

    private LsnPuller() {
        initWorker();
    }

    private static LsnPuller getInstance() {
        if (instance == null) {
            synchronized (LsnPuller.class) {
                if (instance == null) {
                    instance = new LsnPuller();
                }
            }
        }
        return instance;
    }

    private void initWorker() {
        worker = new Worker();
        worker.setName("cdc-LsnPuller");
        worker.setDaemon(false);
        worker.start();
    }

    public static void addExtractor(String metaId, SqlServerListener listener) {
        getInstance().map.put(metaId, listener);
    }

    public static void removeExtractor(String metaId) {
        getInstance().map.remove(metaId);
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    if (map.isEmpty()) {
                        TimeUnit.SECONDS.sleep(1);
                        continue;
                    }
                    Lsn maxLsn = null;
                    for (SqlServerListener listener : map.values()) {
                        maxLsn = listener.getMaxLsn();
                        if (null != maxLsn && maxLsn.isAvailable() && maxLsn.compareTo(listener.getLastLsn()) > 0) {
                            listener.pushStopLsn(maxLsn);
                        }
                    }
                    TimeUnit.MILLISECONDS.sleep(DEFAULT_POLL_INTERVAL_MILLIS);
                } catch (Exception e) {
                    logger.error("异常", e);
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException ex) {
                        logger.warn(ex.getMessage());
                    }
                }
            }
        }

    }

}
