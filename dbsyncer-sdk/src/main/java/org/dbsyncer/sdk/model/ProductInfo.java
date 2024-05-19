/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.sdk.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 产品信息
 *
 * @Author AE86
 * @Version 1.0.0
 * @Date 2024-05-19 10:35
 */
public class ProductInfo {

    /**
     * 公司名称
     */
    private String company;

    /**
     * 负责人
     */
    private String owner;

    /**
     * 手机号
     */
    private String phone;

    /**
     * 备注信息
     */
    private String remark;

    /**
     * 授权时间
     */
    private long createTime;

    /**
     * 授权产品列表明细
     */
    private List<Product> products = new ArrayList<>();

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public List<Product> getProducts() {
        return products;
    }

    public void setProducts(List<Product> products) {
        this.products = products;
    }
}