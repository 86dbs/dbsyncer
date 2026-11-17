/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz;

import java.util.List;
import java.util.Map;

/**
 * 采集任务配置管理
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-11-13
 */
public interface CaptureTaskService {

    /**
     * 列出采集任务（不含私钥）。
     *
     * @param type {@code captureLocal} / {@code captureRemote}；空则返回全部
     */
    List<Map<String, Object>> list(String type);

    /**
     * 按 ID 查询（不含私钥）。
     */
    Map<String, Object> get(String id);

    /**
     * 新增。params 需含 {@code type=captureLocal|captureRemote}。
     */
    String add(Map<String, String> params);

    /**
     * 编辑。运行中不可改。
     */
    String edit(Map<String, String> params);

    /**
     * 删除。运行中不可删。
     */
    void remove(String id);

    /**
     * 生成认证密钥。
     */
    String generateSecret();
}
