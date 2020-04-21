package org.dbsyncer.parser.model;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>驱动同步元信息</p>
 * <pre>
 *     全量同步: 存放分页数
 *     增量同步:定时>时间戳; 日志>binlogFileName/binlogPosition/主从节点信息等
 * </pre>
 *
 * @author AE86
 * @version 1.0.0
 * @date 2020/04/21 16:19
 */
public class Meta extends ConfigModel {

    private AtomicInteger total;
    private AtomicInteger success;
    private AtomicInteger fail;
    private Map<String, String> map;

    public Meta() {
    }

    public Meta(AtomicInteger total, AtomicInteger success, AtomicInteger fail, Map<String, String> map) {
        this.total = total;
        this.success = success;
        this.fail = fail;
        this.map = map;
    }

    public AtomicInteger getTotal() {
        return total;
    }

    public void setTotal(AtomicInteger total) {
        this.total = total;
    }

    public AtomicInteger getSuccess() {
        return success;
    }

    public void setSuccess(AtomicInteger success) {
        this.success = success;
    }

    public AtomicInteger getFail() {
        return fail;
    }

    public void setFail(AtomicInteger fail) {
        this.fail = fail;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }
}