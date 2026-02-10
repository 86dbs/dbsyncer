/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.openapi;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.dbsyncer.common.util.JsonUtil;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * OpenAPI 请求包装类
 * 提供外部系统集成
 *
 * @author 穿云
 * @version 2.0.0
 */
public final class DecryptRequestWrapper extends HttpServletRequestWrapper {

    /** 解密后的参数Map */
    private final Map<String, String[]> parameterMap;

    /** 解密后的数据 */
    private final String decryptedData;

    /** 缓存的请求体 */
    private byte[] cachedBody;

    /**
     * 构造函数
     *
     * @param request 原始请求
     * @param decryptedData 解密后的数据
     */
    public DecryptRequestWrapper(HttpServletRequest request, String decryptedData) {
        super(request);
        this.decryptedData = decryptedData;

        // 将解密后的数据转换为字节数组（使用UTF-8编码）
        if (decryptedData != null) {
            this.cachedBody = decryptedData.getBytes(StandardCharsets.UTF_8);
        } else {
            this.cachedBody = new byte[0];
        }
        // 解析解密后的JSON数据，转换为请求参数
        this.parameterMap = parseJsonToParameters(decryptedData);
    }

    /**
     * 重写getInputStream方法
     * 返回解密后数据的输入流
     */
    @Override
    public ServletInputStream getInputStream() throws IOException {
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(cachedBody);

        return new ServletInputStream() {
            @Override
            public boolean isFinished() {
                return byteArrayInputStream.available() == 0;
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setReadListener(ReadListener listener) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int read() throws IOException {
                return byteArrayInputStream.read();
            }
        };
    }

    /**
     * 重写getReader方法
     * 返回解密后数据的Reader
     */
    @Override
    public BufferedReader getReader() throws IOException {
        return new BufferedReader(new InputStreamReader(getInputStream(), StandardCharsets.UTF_8));
    }

    /**
     * 获取解密后的原始数据
     *
     * @return 解密后的数据
     */
    public String getDecryptedData() {
        return decryptedData;
    }

    /**
     * 获取缓存的请求体字节数组
     *
     * @return 请求体字节数组
     */
    public byte[] getCachedBody() {
        return cachedBody;
    }

    /**
     * 获取请求体长度
     */
    @Override
    public int getContentLength() {
        return cachedBody.length;
    }

    /**
     * 获取请求体长度（long类型）
     */
    @Override
    public long getContentLengthLong() {
        return cachedBody.length;
    }

    /**
     * 获取Content-Type
     * 保持原始的Content-Type
     */
    @Override
    public String getContentType() {
        // 如果解密数据是JSON格式，可以设置为application/json
        // 否则保持原始的Content-Type
        return "application/json;charset=UTF-8";
    }

    /**
     * 获取缓存的请求体字符串（UTF-8编码）
     * 用于在解密前读取原始请求体
     *
     * @return 请求体字符串
     */
    public String getCachedBodyAsUTF8() {
        return new String(cachedBody, StandardCharsets.UTF_8);
    }

    /**
     * 解析JSON数据为请求参数
     */
    private Map<String, String[]> parseJsonToParameters(String json) {
        Map<String, String[]> params = new HashMap<>();

        JSONObject jsonObject = JSONObject.parseObject(json);

        // 遍历JSON所有字段，转换为请求参数
        for (String key : jsonObject.keySet()) {
            Object value = jsonObject.get(key);

            if (value == null) {
                continue;
            }

            // 根据值的类型处理
            if (value instanceof JSONArray) {
                // 数组类型
                JSONArray jsonArray = (JSONArray) value;
                String[] values = new String[jsonArray.size()];
                for (int i = 0; i < jsonArray.size(); i++) {
                    values[i] = jsonArray.getString(i);
                }
                params.put(key, values);

            } else if (value instanceof Map) {
                // 对象类型，转换为JSON字符串
                String jsonValue = JsonUtil.objToJson(value);
                params.put(key, new String[]{jsonValue});

            } else {
                // 基本类型
                String strValue = String.valueOf(value);
                params.put(key, new String[]{strValue});
            }
        }
        return params;
    }

    /**
     * 重写getParameterMap
     * 合并原始参数和解密参数
     */
    @Override
    public Map<String, String[]> getParameterMap() {
        Map<String, String[]> originalParams = super.getParameterMap();

        Map<String, String[]> mergedParams = new HashMap<>();

        // 先添加原始参数
        if (originalParams != null) {
            mergedParams.putAll(originalParams);
        }

        // 再添加解密参数（覆盖同名的原始参数）
        mergedParams.putAll(parameterMap);

        return mergedParams;
    }

    /**
     * 重写getParameter
     */
    @Override
    public String getParameter(String name) {
        // 先从解密参数中查找
        String[] values = parameterMap.get(name);
        if (values != null && values.length > 0) {
            return values[0];
        }

        // 没有找到，从原始参数中查找
        return super.getParameter(name);
    }

    /**
     * 重写getParameterValues
     */
    @Override
    public String[] getParameterValues(String name) {
        // 先从解密参数中查找
        String[] values = parameterMap.get(name);
        if (values != null && values.length > 0) {
            return values;
        }

        // 没有找到，从原始参数中查找
        return super.getParameterValues(name);
    }

    /**
     * 重写getParameterNames
     */
    @Override
    public Enumeration<String> getParameterNames() {
        Map<String, String[]> mergedMap = getParameterMap();
        return Collections.enumeration(mergedMap.keySet());
    }
}
