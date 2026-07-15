/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author 穿云
 * @Version 1.0.0
 * @Date 2025-07-04 22:38
 */
public class EditionInfoVO {

    private String edition;
    private String editionName;
    private String editionStatus;
    private long effectiveTime;
    private long currentTime = System.currentTimeMillis();
    private String effectiveContent;
    private int attentionCount;
    private String attentionSummary;
    private List<ProductStatusVO> products = new ArrayList<>();

    public String getEdition() {
        return edition;
    }

    public void setEdition(String edition) {
        this.edition = edition;
    }

    public String getEditionName() {
        return editionName;
    }

    public void setEditionName(String editionName) {
        this.editionName = editionName;
    }

    public long getEffectiveTime() {
        return effectiveTime;
    }

    public void setEffectiveTime(long effectiveTime) {
        this.effectiveTime = effectiveTime;
    }

    public long getCurrentTime() {
        return currentTime;
    }

    public void setCurrentTime(long currentTime) {
        this.currentTime = currentTime;
    }

    public String getEffectiveContent() {
        return effectiveContent;
    }

    public void setEffectiveContent(String effectiveContent) {
        this.effectiveContent = effectiveContent;
    }

    public String getEditionStatus() {
        return editionStatus;
    }

    public void setEditionStatus(String editionStatus) {
        this.editionStatus = editionStatus;
    }

    public int getAttentionCount() {
        return attentionCount;
    }

    public void setAttentionCount(int attentionCount) {
        this.attentionCount = attentionCount;
    }

    public String getAttentionSummary() {
        return attentionSummary;
    }

    public void setAttentionSummary(String attentionSummary) {
        this.attentionSummary = attentionSummary;
    }

    public List<ProductStatusVO> getProducts() {
        return products;
    }

    public void setProducts(List<ProductStatusVO> products) {
        this.products = products;
    }
}
