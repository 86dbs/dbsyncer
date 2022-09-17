package org.dbsyncer.listener.sqlserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author Xinpeng.Fu
 * @version V1.0
 * @description
 * @date 2022/8/30 10:04
 */
public class LsnPuller {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 间隔拉取最新LSN时间（毫秒）
     */
    private static final long DEFAULT_POLL_INTERVAL_MILLIS = 100;
    private static volatile LsnPuller instance = null;
    private final Map<String, SqlServerExtractor> map = new ConcurrentHashMap<>();
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

    public static void addExtractor(String metaId, SqlServerExtractor extractor) {
        getInstance().map.put(metaId, extractor);
    }

    public static void removeExtractor(String metaId) {
        getInstance().map.remove(metaId);
    }

    final class Worker extends Thread {

        private final Map<String, Lsn> maxLsnSnapshot = new LinkedCaseInsensitiveMap<>();

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    if (map.isEmpty()) {
                        TimeUnit.SECONDS.sleep(1);
                        continue;
                    }
                    maxLsnSnapshot.clear();
                    Lsn maxLsn = null;
                    for (SqlServerExtractor extractor : map.values()) {
                        maxLsn = getMaxLsn(maxLsnSnapshot, extractor);
                        if (null != maxLsn && maxLsn.isAvailable() && maxLsn.compareTo(extractor.getLastLsn()) > 0) {
                            extractor.pushStopLsn(maxLsn);
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

    private Lsn getMaxLsn(Map<String, Lsn> maxLsnSnapshot, SqlServerExtractor extractor) {
        final String url = extractor.getDatabaseConfigUrl();
        if (maxLsnSnapshot.containsKey(url)) {
            return maxLsnSnapshot.get(url);
        }

        Lsn maxLsn = extractor.getMaxLsn();
        maxLsnSnapshot.put(url, maxLsn);
        return maxLsn;
    }

}
