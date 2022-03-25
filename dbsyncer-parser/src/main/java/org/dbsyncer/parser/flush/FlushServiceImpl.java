package org.dbsyncer.parser.flush;

import com.alibaba.fastjson.JSONException;
import org.dbsyncer.common.scheduled.ScheduledTaskJob;
import org.dbsyncer.common.scheduled.ScheduledTaskService;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.storage.SnowflakeIdWorker;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.enums.StorageEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 持久化
 * <p>全量或增量数据</p>
 * <p>系统日志</p>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/05/19 18:38
 */
@Component
public class FlushServiceImpl implements FlushService, ScheduledTaskJob, DisposableBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private StorageService storageService;

    @Autowired
    private SnowflakeIdWorker snowflakeIdWorker;

    @Autowired
    private ScheduledTaskService scheduledTaskService;

    private String key;

    @PostConstruct
    private void init() {
        key = UUIDUtil.getUUID();
        String cron = "*/3 * * * * ?";
        scheduledTaskService.start(key, cron, this);
        logger.info("[{}], Started scheduled task", cron);
    }

    @Override
    public void asyncWrite(String type, String error) {
        Map<String, Object> params = new HashMap();
        params.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(snowflakeIdWorker.nextId()));
        params.put(ConfigConstant.CONFIG_MODEL_TYPE, type);
        params.put(ConfigConstant.CONFIG_MODEL_JSON, error);
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, Instant.now().toEpochMilli());
        storageService.addLog(StorageEnum.LOG, params);
    }

    @Override
    public void asyncWrite(String metaId, String event, boolean success, List<Map> data, String error) {
        long now = Instant.now().toEpochMilli();
        AtomicBoolean added = new AtomicBoolean(false);
        List<Map> list = data.parallelStream().map(r -> {
            Map<String, Object> params = new HashMap();
            params.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(snowflakeIdWorker.nextId()));
            params.put(ConfigConstant.DATA_SUCCESS, success ? StorageDataStatusEnum.SUCCESS.getValue() : StorageDataStatusEnum.FAIL.getValue());
            params.put(ConfigConstant.DATA_EVENT, event);
            params.put(ConfigConstant.DATA_ERROR, added.get() ? "" : error);
            try {
                params.put(ConfigConstant.CONFIG_MODEL_JSON, JsonUtil.objToJson(r));
            } catch (JSONException e) {
                logger.warn("可能存在Blob或inputStream大文件类型, 无法序列化:{}", r);
                params.put(ConfigConstant.CONFIG_MODEL_JSON, r.toString());
            }
            params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, now);
            added.set(true);
            return params;
        }).collect(Collectors.toList());
        storageService.addData(StorageEnum.DATA, metaId, list);
    }


    @Override
    public void run() {
        // TODO 批量写入同步数据
        logger.info("run flush task");
    }

    @Override
    public void destroy() {
        scheduledTaskService.stop(key);
        logger.info("Stopped scheduled task.");
    }
}