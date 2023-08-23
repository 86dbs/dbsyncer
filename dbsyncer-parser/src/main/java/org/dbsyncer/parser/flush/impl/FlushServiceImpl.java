package org.dbsyncer.parser.flush.impl;

import org.dbsyncer.common.config.IncrementDataConfig;
import org.dbsyncer.common.snowflake.SnowflakeIdWorker;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.flush.BufferActuator;
import org.dbsyncer.parser.flush.FlushService;
import org.dbsyncer.parser.model.StorageRequest;
import org.dbsyncer.storage.StorageService;
import org.dbsyncer.storage.constant.ConfigConstant;
import org.dbsyncer.storage.enums.StorageDataStatusEnum;
import org.dbsyncer.storage.enums.StorageEnum;
import org.dbsyncer.storage.util.BinlogMessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @Resource
    private StorageService storageService;

    @Resource
    private SnowflakeIdWorker snowflakeIdWorker;

    @Resource
    private BufferActuator storageBufferActuator;

    @Resource
    private IncrementDataConfig flushDataConfig;

    @Override
    public void asyncWrite(String type, String error) {
        Map<String, Object> params = new HashMap();
        params.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(snowflakeIdWorker.nextId()));
        params.put(ConfigConstant.CONFIG_MODEL_TYPE, type);
        params.put(ConfigConstant.CONFIG_MODEL_JSON, substring(error));
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, Instant.now().toEpochMilli());
        storageService.add(StorageEnum.LOG, params);
    }

    @Override
    public void write(String metaId, String tableGroupId, String targetTableGroupName, String event, boolean success, List<Map> data, String error) {
        long now = Instant.now().toEpochMilli();
        data.forEach(r -> {
            Map<String, Object> row = new HashMap();
            row.put(ConfigConstant.CONFIG_MODEL_ID, String.valueOf(snowflakeIdWorker.nextId()));
            row.put(ConfigConstant.DATA_SUCCESS, success ? StorageDataStatusEnum.SUCCESS.getValue() : StorageDataStatusEnum.FAIL.getValue());
            row.put(ConfigConstant.DATA_TABLE_GROUP_ID, tableGroupId);
            row.put(ConfigConstant.DATA_TARGET_TABLE_NAME, targetTableGroupName);
            row.put(ConfigConstant.DATA_EVENT, event);
            row.put(ConfigConstant.DATA_ERROR, substring(error));
            row.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, now);
            try {
                byte[] bytes = BinlogMessageUtil.toBinlogMap(r).toByteArray();
                row.put(ConfigConstant.BINLOG_DATA, bytes);
            } catch (Exception e) {
                logger.warn("可能存在Blob或inputStream大文件类型, 无法序列化:{}", r);
            }

            storageBufferActuator.offer(new StorageRequest(metaId, row));
        });
    }

    /**
     * 限制记录异常信息长度
     *
     * @param error
     * @return
     */
    private String substring(String error) {
        return StringUtil.substring(error, 0, flushDataConfig.getMaxErrorLength());
    }

}