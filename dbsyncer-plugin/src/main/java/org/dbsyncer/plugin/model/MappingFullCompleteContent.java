/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.model;

import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.enums.NoticeTypeEnum;
import org.dbsyncer.sdk.model.NoticeContent;

/**
 * 全量同步完成通知
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-27
 */
public final class MappingFullCompleteContent extends NoticeContent {

    private String name;

    private ModelEnum model;

    private long success;

    private long fail;

    private long total;

    public MappingFullCompleteContent() {
        setNoticeType(NoticeTypeEnum.MAPPING_FULL_COMPLETE);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ModelEnum getModel() {
        return model;
    }

    public void setModel(ModelEnum model) {
        this.model = model;
    }

    public long getSuccess() {
        return success;
    }

    public void setSuccess(long success) {
        this.success = success;
    }

    public long getFail() {
        return fail;
    }

    public void setFail(long fail) {
        this.fail = fail;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }
}
