/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.util;

import org.dbsyncer.parser.model.Meta;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Meta 启动时间落库与耗时映射。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-08-18
 */
public class ConfigModelUtilTest {

    @Test
    public void convertMetaPersistsBeginTimeAsStartTime() {
        Meta meta = new Meta();
        meta.setId("m1");
        meta.setTaskId("t1");
        meta.setBeginTime(1000L);
        meta.setEndTime(5000L);
        meta.setUpdateTime(4000L);

        Map<String, Object> params = ConfigModelUtil.convertModelToMap(meta);

        Assert.assertEquals(1000L, params.get(ConfigConstant.META_START_TIME));
        Assert.assertEquals(4000L, params.get(ConfigConstant.CONFIG_MODEL_UPDATE_TIME));
        Assert.assertNull(params.get("beginTime"));
        Assert.assertNull(params.get("endTime"));
        Assert.assertNull(params.get("epoch"));
        Assert.assertNull(params.get("leaseOwner"));
        Assert.assertNull(params.get("leaseExpireAt"));
    }

    @Test
    public void parseMetaMapsStartTimeAndUpdateTimeToDuration() {
        Map<String, Object> row = new HashMap<>();
        row.put(ConfigConstant.CONFIG_MODEL_ID, "m1");
        row.put(ConfigConstant.META_TASK_ID, "t1");
        row.put(ConfigConstant.META_START_TIME, 1000L);
        row.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, 4000L);
        row.put(ConfigConstant.META_SNAPSHOT, "{}");
        row.put(ConfigConstant.META_TOTAL, 0L);
        row.put(ConfigConstant.META_SUCCESS, 0L);
        row.put(ConfigConstant.META_FAIL, 0L);
        row.put(ConfigConstant.META_DIFF, 0L);
        row.put(ConfigConstant.META_FIXED, 0L);

        Meta meta = ConfigModelUtil.parseFromRow(row, Meta.class);

        Assert.assertEquals(1000L, meta.getBeginTime());
        Assert.assertEquals(4000L, meta.getEndTime());
    }
}
