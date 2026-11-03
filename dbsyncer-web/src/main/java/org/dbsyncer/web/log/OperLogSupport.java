/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.log;

import org.dbsyncer.biz.enums.SafeInfoEnum;
import org.dbsyncer.common.enums.StatusEnum;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;

import javax.servlet.http.HttpServletRequest;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 操作日志参数采集与脱敏。
 *
 * @author wuji
 * @version 1.0.0
 */
public abstract class OperLogSupport {

    public static final String ATTR_CACHED_REQUEST = "dbsyncer.operLog.cachedRequest";
    public static final String ATTR_CACHED_RESPONSE = "dbsyncer.operLog.cachedResponse";
    public static final String ATTR_START_MS = "dbsyncer.operLog.startMs";

    public static Map<String, Object> collectRequestParams(HttpServletRequest request) {
        Map<String, Object> params = new LinkedHashMap<>();
        Map<String, String[]> parameterMap = request.getParameterMap();
        if (!CollectionUtils.isEmpty(parameterMap)) {
            for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
                String[] values = entry.getValue();
                if (values == null || values.length == 0) {
                    params.put(entry.getKey(), StringUtil.EMPTY);
                } else if (values.length == 1) {
                    params.put(entry.getKey(), values[0]);
                } else {
                    params.put(entry.getKey(), values);
                }
            }
        }
        if (request instanceof ContentCachingRequestWrapper && params.isEmpty()) {
            ContentCachingRequestWrapper wrapper = (ContentCachingRequestWrapper) request;
            String body = readBody(wrapper.getContentAsByteArray(), resolveBodyCharset(wrapper.getCharacterEncoding()));
            if (StringUtil.isNotBlank(body)) {
                Object parsed = tryParseJson(body);
                if (parsed instanceof Map) {
                    params.putAll((Map<String, Object>) parsed);
                } else {
                    params.put("body", body);
                }
            }
        }
        return sanitize(params);
    }

    public static Object collectResponseParam(ContentCachingResponseWrapper response) {
        if (response == null) {
            return Collections.emptyMap();
        }
        return sanitizeResponse(readBody(response.getContentAsByteArray(), resolveBodyCharset(response.getCharacterEncoding())));
    }

    public static int resolveStatus(Object responseParam, int httpStatus) {
        if (responseParam instanceof Map) {
            Object success = ((Map<?, ?>) responseParam).get("success");
            if (success instanceof Boolean) {
                return StatusEnum.of(Boolean.TRUE.equals(success)).getCode();
            }
        }
        return StatusEnum.of(httpStatus < 400).getCode();
    }

    public static String normalizePath(HttpServletRequest request) {
        String uri = request.getRequestURI();
        String context = request.getContextPath();
        if (StringUtil.isNotBlank(context) && uri.startsWith(context)) {
            uri = uri.substring(context.length());
        }
        return StringUtil.isBlank(uri) ? "/" : uri;
    }

    /**
     * 序列化 密码不记录明文
     */
    public static Map<String, Object> sanitize(Map<String, Object> source) {
        if (CollectionUtils.isEmpty(source)) {
            return Collections.emptyMap();
        }
        Map<String, Object> target = new HashMap<>(source.size());
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            if (SafeInfoEnum.contains(key)) {
                target.put(key, "***");
            } else if (entry.getValue() instanceof Map) {
                target.put(key, sanitize((Map<String, Object>) entry.getValue()));
            } else {
                target.put(key, entry.getValue());
            }
        }
        return target;
    }

    private static Object sanitizeResponse(String body) {
        if (StringUtil.isBlank(body)) {
            return Collections.emptyMap();
        }
        Object parsed = tryParseJson(body);
        if (parsed instanceof Map) {
            return sanitize((Map<String, Object>) parsed);
        }
        return body;
    }

    private static Object tryParseJson(String body) {
        String trimmed = body.trim();
        if (!(trimmed.startsWith("{") || trimmed.startsWith("["))) {
            return body;
        }
        try {
            if (trimmed.startsWith("{")) {
                return JsonUtil.parseMap(trimmed);
            }
            return JsonUtil.jsonToArray(trimmed, Object.class);
        } catch (Exception e) {
            return body;
        }
    }

    private static String readBody(byte[] bytes, String encoding) {
        if (bytes == null || bytes.length == 0) {
            return StringUtil.EMPTY;
        }
        Charset charset = StandardCharsets.UTF_8;
        if (StringUtil.isNotBlank(encoding)) {
            try {
                charset = Charset.forName(encoding);
            } catch (Exception ignore) {
                // keep utf-8
            }
        }
        return new String(bytes, charset);
    }

    /**
     * Servlet 默认编码为 ISO-8859-1，与本系统 UTF-8 JSON 不一致时回退 UTF-8，避免中文乱码。
     */
    private static String resolveBodyCharset(String encoding) {
        if (StringUtil.isBlank(encoding) || "ISO-8859-1".equalsIgnoreCase(encoding)) {
            return StandardCharsets.UTF_8.name();
        }
        return encoding;
    }
}
