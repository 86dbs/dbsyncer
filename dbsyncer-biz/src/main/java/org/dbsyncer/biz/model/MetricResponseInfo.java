package org.dbsyncer.biz.model;

public class MetricResponseInfo {

    private MetricResponse response;

    private long queueUp;

    public MetricResponse getResponse() {
        return response;
    }

    public void setResponse(MetricResponse response) {
        this.response = response;
    }

    public long getQueueUp() {
        return queueUp;
    }

    public void setQueueUp(long queueUp) {
        this.queueUp = queueUp;
    }
}