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

import java.util.*;

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
    private StorageService    storageService;

    @Autowired
    private SnowflakeIdWorker snowflakeIdWorker;

    @Override
    public void asyncWrite(String metaId, String error) {
        Map<String, Object> params = new HashMap();
        params.put(ConfigConstant.CONFIG_MODEL_ID, snowflakeIdWorker.nextId());
        params.put(ConfigConstant.CONFIG_MODEL_TYPE, metaId);
        params.put(ConfigConstant.CONFIG_MODEL_JSON, error);
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, System.currentTimeMillis());
        storageService.addLog(StorageEnum.LOG, params);
    }

    @Override
    public void asyncWrite(String metaId, Queue<Map<String, Object>> data) {
        List<Map> list = new LinkedList<>();
        long now = System.currentTimeMillis();
        Map<String, Object> params = null;
        while (!data.isEmpty()){
            params = new HashMap();
            params.put(ConfigConstant.CONFIG_MODEL_ID, snowflakeIdWorker.nextId());
            params.put(ConfigConstant.CONFIG_MODEL_JSON, JsonUtil.objToJson(data.poll()));
            params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, now);
            list.add(params);
        }
        storageService.addData(StorageEnum.DATA, metaId, list);
    }
}