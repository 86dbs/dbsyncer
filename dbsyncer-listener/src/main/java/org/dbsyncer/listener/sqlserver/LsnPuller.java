package org.dbsyncer.listener.sqlserver;

import org.dbsyncer.connector.config.DatabaseConfig;
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

    private static final long DEFAULT_POLL_INTERVAL_MILLIS = 100;
    private static final long WAIT_INTERVAL_MILLIS = 1000;
    private Worker worker;
    private static volatile LsnPuller instance = null;

    private Map<String, SqlServerExtractor> map = new ConcurrentHashMap<>();

    private LsnPuller(){
        initWorker();
    }

    private static LsnPuller getInstance(){
        if(instance == null){
            synchronized (LsnPuller.class){
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

    public static void addExtractor(String metaId, SqlServerExtractor extractor){
        getInstance().map.put(metaId, extractor);
    }


    public static void removeExtractor(String metaId) {
        getInstance().map.remove(metaId);
    }

    final class Worker extends Thread {

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    if(map.isEmpty()){
                        TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MILLIS);
                        continue;
                    }
                    Map<String, Lsn> dataBaseMaxLsn = new LinkedCaseInsensitiveMap<>();
                    for (SqlServerExtractor extractor : map.values()) {
                        if(extractor.getLastLsn() == null){
                            continue;
                        }
                        DatabaseConfig connectorConfig = extractor.getConnectorConfig();
                        String url = connectorConfig.getUrl();
                        Lsn maxLsn = null;
                        if(dataBaseMaxLsn.containsKey(url)){
                            maxLsn = dataBaseMaxLsn.get(url);
                        }else{
                            try{
                                maxLsn = extractor.getMaxLsn();
                                dataBaseMaxLsn.put(url, maxLsn);
                            }catch (Exception e) {
                                logger.error("获取maxLsn异常：", e);
                            }
                        }
                        if (null != maxLsn && maxLsn.isAvailable() && maxLsn.compareTo(extractor.getLastLsn()) > 0) {
                            extractor.pushStopLsn(maxLsn);
                        }
                    }
                    TimeUnit.MILLISECONDS.sleep(DEFAULT_POLL_INTERVAL_MILLIS);
                }catch (InterruptedException ex){
                    logger.warn(ex.getMessage());
                }catch (Exception e) {
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
