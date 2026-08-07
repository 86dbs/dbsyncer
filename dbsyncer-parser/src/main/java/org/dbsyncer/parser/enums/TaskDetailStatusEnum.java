/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.enums;

import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;

/**
 * 任务明细列表状态筛选（success / fail）。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-07-31 18:20
 */
public enum TaskDetailStatusEnum {

    /**
     * 成功（对应指标列 = 0）
     */
    SUCCESS(ConfigConstant.META_SUCCESS),

    /**
     * 失败（对应指标列 &gt; 0）
     */
    FAIL(ConfigConstant.META_FAIL);

    private final String code;

    TaskDetailStatusEnum(String code) {
        this.code = code;
    }

    /**
     * 解析请求参数；空白或无法识别时返回 {@code null}（表示不按状态筛选）。
     *
     * @param value success / fail（忽略大小写）
     * @return 枚举或 null
     */
    public static TaskDetailStatusEnum from(String value) {
        if (StringUtil.isBlank(value)) {
            return null;
        }
        for (TaskDetailStatusEnum status : values()) {
            if (StringUtil.equalsIgnoreCase(status.code, value)) {
                return status;
            }
        }
        return null;
    }

    public String getCode() {
        return code;
    }
}
