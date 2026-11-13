/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.common.util.UUIDUtil;
import org.dbsyncer.parser.CaptureTaskRelProfile;
import org.dbsyncer.parser.model.CaptureTaskRel;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link CaptureTaskRelProfile} 实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-12
 */
@Component
public class CaptureTaskRelProfileImpl implements CaptureTaskRelProfile {

    private static final String TABLE = "dbsyncer_capture_task_rel";

    private static final String COLUMNS =
            "ID AS id, CREATE_TIME AS createTime, UPDATE_TIME AS updateTime,"
                    + " CAPTURE_TASK_ID AS captureTaskId, TASK_ID AS taskId";

    @Resource
    private StorageService storageService;

    @Override
    public CaptureTaskRel getByTaskId(String taskId) {
        if (StringUtil.isBlank(taskId)) {
            return null;
        }
        List<CaptureTaskRel> list = storageService.query(
                "SELECT " + COLUMNS + " FROM " + TABLE + " WHERE TASK_ID=? LIMIT 1",
                new Object[]{taskId}, CaptureTaskRel.class);
        return CollectionUtils.isEmpty(list) ? null : list.get(0);
    }

    @Override
    public List<CaptureTaskRel> listByCaptureTaskId(String captureTaskId) {
        if (StringUtil.isBlank(captureTaskId)) {
            return Collections.emptyList();
        }
        List<CaptureTaskRel> list = storageService.query(
                "SELECT " + COLUMNS + " FROM " + TABLE + " WHERE CAPTURE_TASK_ID=?",
                new Object[]{captureTaskId}, CaptureTaskRel.class);
        return list == null ? Collections.emptyList() : list;
    }

    @Override
    public boolean add(CaptureTaskRel rel) {
        long now = System.currentTimeMillis();
        if (StringUtil.isBlank(rel.getId())) {
            rel.setId(UUIDUtil.getUUID());
        }
        if (rel.getCreateTime() == null) {
            rel.setCreateTime(now);
        }
        rel.setUpdateTime(now);
        try {
            storageService.add(StorageEnum.CAPTURE_TASK_REL, toMap(rel));
            return true;
        } catch (RuntimeException e) {
            if (isDuplicateKey(e)) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public int removeByTaskId(String taskId) {
        if (StringUtil.isBlank(taskId)) {
            return 0;
        }
        return storageService.update("DELETE FROM " + TABLE + " WHERE TASK_ID=?", new Object[]{taskId});
    }

    @Override
    public int removeByCaptureTaskId(String captureTaskId) {
        if (StringUtil.isBlank(captureTaskId)) {
            return 0;
        }
        return storageService.update("DELETE FROM " + TABLE + " WHERE CAPTURE_TASK_ID=?",
                new Object[]{captureTaskId});
    }

    private boolean isDuplicateKey(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof DuplicateKeyException) {
                return true;
            }
            String msg = t.getMessage();
            if (msg != null && msg.contains("Duplicate")) {
                return true;
            }
        }
        return false;
    }

    private Map<String, Object> toMap(CaptureTaskRel rel) {
        Map<String, Object> params = new HashMap<>();
        params.put(ConfigConstant.CONFIG_MODEL_ID, rel.getId());
        params.put(ConfigConstant.CAPTURE_TASK_ID, rel.getCaptureTaskId());
        params.put(ConfigConstant.TASK_ID, rel.getTaskId());
        params.put(ConfigConstant.CONFIG_MODEL_CREATE_TIME, rel.getCreateTime());
        params.put(ConfigConstant.CONFIG_MODEL_UPDATE_TIME, rel.getUpdateTime());
        return params;
    }
}
