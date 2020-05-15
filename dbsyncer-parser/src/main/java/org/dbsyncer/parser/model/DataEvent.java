package org.dbsyncer.parser.model;

import org.dbsyncer.common.util.JsonUtil;

import java.util.Map;

public final class DataEvent {

    private String event;
    private Map<String, Object> before;
    private Map<String, Object> after;

    public DataEvent(String event, Map<String, Object> before, Map<String, Object> after) {
        this.event = event;
        this.before = before;
        this.after = after;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    @Override
    public String toString() {
        return JsonUtil.objToJson(this);
    }
}