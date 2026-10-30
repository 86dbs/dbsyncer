package org.dbsyncer.biz.vo;

/**
 * 日志列表/详情视图。
 */
public class LogVO {

    private String id;
    private String type;
    /**
     * 模块名（由 type 解析）
     */
    private String typeName;
    private String username;
    private String ip;
    /**
     * 0失败 1成功
     */
    private Integer status;
    private String json;
    /**
     * 列表摘要（优先 JSON.message，否则原文）
     */
    private String message;
    private String requestUri;
    private Object requestParam;
    private Object responseParam;
    private Long costMs;
    private long createTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getRequestUri() {
        return requestUri;
    }

    public void setRequestUri(String requestUri) {
        this.requestUri = requestUri;
    }

    public Object getRequestParam() {
        return requestParam;
    }

    public void setRequestParam(Object requestParam) {
        this.requestParam = requestParam;
    }

    public Object getResponseParam() {
        return responseParam;
    }

    public void setResponseParam(Object responseParam) {
        this.responseParam = responseParam;
    }

    public Long getCostMs() {
        return costMs;
    }

    public void setCostMs(Long costMs) {
        this.costMs = costMs;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
}
