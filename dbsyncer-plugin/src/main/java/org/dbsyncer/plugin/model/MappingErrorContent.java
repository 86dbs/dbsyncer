/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.plugin.model;

import org.dbsyncer.sdk.enums.ModelEnum;
import org.dbsyncer.sdk.enums.NoticeTypeEnum;
import org.dbsyncer.sdk.model.NoticeContent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 驱动异常信息
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2026-03-04 19:00
 */
public final class MappingErrorContent extends NoticeContent {

    private List<ErrorItem> errorItems;

    public MappingErrorContent() {
        setNoticeType(NoticeTypeEnum.MAPPING_ERROR);
    }

    public void addErrorItem(ErrorItem item) {
        if (errorItems == null) {
            errorItems = new ArrayList<>();
        }
        errorItems.add(item);
    }

    public List<ErrorItem> getErrorItems() {
        return errorItems;
    }

    public static class ErrorItem {
        private String name;
        private ModelEnum model;
        private AtomicLong total;
        private AtomicLong success;
        private AtomicLong fail;

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

        public AtomicLong getTotal() {
            return total;
        }

        public void setTotal(AtomicLong total) {
            this.total = total;
        }

        public AtomicLong getSuccess() {
            return success;
        }

        public void setSuccess(AtomicLong success) {
            this.success = success;
        }

        public AtomicLong getFail() {
            return fail;
        }

        public void setFail(AtomicLong fail) {
            this.fail = fail;
        }
    }

}
