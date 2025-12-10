package org.dbsyncer.connector.sqlserver.ct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Change Tracking 版本号轮询器（单例）
 * 类似于 LsnPuller，但用于 Change Tracking 版本号
 */
public class VersionPuller {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 间隔拉取最新版本号时间（毫秒）
     */
    private static final long DEFAULT_POLL_INTERVAL_MILLIS = 100;
    private static volatile VersionPuller instance = null;
    private final Map<String, SqlServerCTListener> map = new ConcurrentHashMap<>();
    private Worker worker;

    private VersionPuller() {
        initWorker();
    }

    private static VersionPuller getInstance() {
        if (instance == null) {
            synchronized (VersionPuller.class) {
                if (instance == null) {
                    instance = new VersionPuller();
                }
            }
        }
        return instance;
    }

    private void initWorker() {
        worker = new Worker();
        worker.setName("ct-VersionPuller");
        worker.setDaemon(false);
        worker.start();
    }

    public static void addExtractor(String metaId, SqlServerCTListener listener) {
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
                    for (SqlServerCTListener listener : map.values()) {
                        try {
                            Long maxVersion = listener.getMaxVersion();
                            if (maxVersion != null && maxVersion > listener.getLastVersion()) {
                                listener.pushStopVersion(maxVersion);
                            }
                        } catch (Exception e) {
                            logger.error("VersionPuller 轮询失败: {}", e.getMessage(), e);
                        }
                    }
                    TimeUnit.MILLISECONDS.sleep(DEFAULT_POLL_INTERVAL_MILLIS);
                } catch (InterruptedException e) {
                    logger.warn("VersionPuller 被中断: {}", e.getMessage());
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("VersionPuller 异常", e);
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException ex) {
                        logger.warn(ex.getMessage());
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
    }
}

