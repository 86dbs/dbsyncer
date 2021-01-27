package org.dbsyncer.parser.flush;

import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.storage.SnowflakeIdWorker;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
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
public class FlushServiceImpl implements FlushService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private StorageService storageService;

    @Autowired
    private SnowflakeIdWorker snowflakeIdWorker;

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
    public void asyncWrite(String metaId, String event, boolean success, List<Map<String, Object>> data, String error) {
        long now = Instant.now().toEpochMilli();
        List<Map> list = data.parallelStream().map(r -> {
            Map<String, Object> params = new HashMap();
            params.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(snowflakeIdWorker.nextId()));
            params.put(ConfigConstant.DATA_SUCCESS, success ? 1 : 0);
            params.put(ConfigConstant.DATA_EVENT, event);
            params.put(ConfigConstant.DATA_ERROR, error);
            params.put(ConfigConstant.CONFIG_MODEL_JSON, JsonUtil.objToJson(r));
            params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, now);
            return params;
        }).collect(Collectors.toList());
        storageService.addData(StorageEnum.DATA, metaId, list);
    }
}