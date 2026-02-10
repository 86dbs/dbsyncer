/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.openapi;

import com.alibaba.fastjson2.JSON;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.impl.ApiKeyManager;
import org.dbsyncer.biz.impl.JwtSecretManager;
import org.dbsyncer.biz.impl.RsaManager;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.model.OpenApiData;
import org.dbsyncer.common.model.RsaConfig;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.web.model.OpenApiResponse;
import org.dbsyncer.web.security.TimestampValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.method.support.HandlerMethodArgumentResolverComposite;
import org.springframework.web.method.support.InvocableHandlerMethod;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.springframework.web.servlet.mvc.condition.PatternsRequestCondition;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;
import org.springframework.web.servlet.support.RequestContextUtils;
import org.springframework.web.util.UrlPathHelper;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OpenAPI控制器
 * 提供外部系统集成接口
 *
 * @author 穿云
 * @version 2.0.0
 */
@RestController
@RequestMapping("/openapi")
public class OpenApiController implements InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private SystemConfigService systemConfigService;

    @Resource
    private JwtSecretManager jwtSecretManager;

    @Resource
    private ApiKeyManager apiKeyManager;

    @Resource
    private RsaManager rsaManager;

    @Resource
    private RequestMappingHandlerAdapter requestMappingHandlerAdapter;

    @Resource
    private ApplicationContext applicationContext;

    private final Map<String, InvocableHandlerMethod> handlers = new ConcurrentHashMap<>();
    /**
     * 各 path 允许的 HTTP 方法（空表示不限制），用于 GET/POST 校验
     */
    private final Map<String, Set<RequestMethod>> handlerAllowedMethods = new ConcurrentHashMap<>();
    private final HandlerMethodArgumentResolverComposite resolvers = new HandlerMethodArgumentResolverComposite();
    private final UrlPathHelper urlPathHelper = new UrlPathHelper();

    private static final String OPENAPI_V1_PREFIX = "/openapi/v1";

    // 是否公网场景（可通过配置或请求头判断）
    private static final String PUBLIC_NETWORK_HEADER = "X-Public-Network";

    /**
     * 模拟客户端请求接口
     *
     * @param request 请求对象
     * @return 同步结果
     */
    @PostMapping("/mock")
    public Object mock(HttpServletRequest request) {
        try {
            String requestBody = readRequestBody(request);
            boolean isPublicNetwork = isPublicNetwork(request);
            RsaConfig rsaConfig = systemConfigService.getSystemConfig().getRsaConfig();
            return rsaManager.encrypt(rsaConfig, requestBody, isPublicNetwork);
        } catch (Exception e) {
            logger.error("mock失败", e);
            return OpenApiResponse.fail(OpenApiErrorCode.INTERNAL_ERROR, "mock失败: " + e.getMessage());
        }
    }

    /**
     * OpenAPI v1 统一入口，将 /openapi/v1/xxx 转发到内部 Controller 的 /xxx
     * <p>
     * 示例：<br>
     * - /openapi/v1/connector/test → 转发到 /connector/test<br>
     * - /openapi/v1/test → 转发到 /test
     * </p>
     */
    @RequestMapping(value = "/v1/**")
    public Object gateway(HttpServletRequest request, HttpServletResponse response) {
        try {
            String lookupPath = getV1LookupPath(request);
            if (lookupPath == null) {
                return OpenApiResponse.fail(OpenApiErrorCode.BAD_REQUEST, "请求路径无效");
            }
            InvocableHandlerMethod invocableMethod = handlers.get(lookupPath);
            if (invocableMethod == null) {
                logger.warn("OpenAPI v1 未找到对应接口: {}", lookupPath);
                return OpenApiResponse.fail(OpenApiErrorCode.NOT_FOUND, "接口不存在: " + lookupPath);
            }

            // GET/POST 等请求方法校验：请求方法必须在目标 handler 允许的方法列表中
            Set<RequestMethod> allowedMethods = handlerAllowedMethods.get(lookupPath);
            if (allowedMethods != null && !allowedMethods.isEmpty()) {
                RequestMethod requestMethod = parseRequestMethod(request.getMethod());
                if (requestMethod == null || !allowedMethods.contains(requestMethod)) {
                    logger.warn("OpenAPI v1 Method Not Allowed: path={}", lookupPath);
                    return OpenApiResponse.fail(OpenApiErrorCode.METHOD_NOT_ALLOWED, "Method Not Allowed");
                }
            }

            // 解密入参
            String requestBody = readRequestBody(request);
            Assert.hasText(requestBody, "请求体不能为空");
            // 先解析为OpenApiRequest格式（包含时间戳和nonce）
            OpenApiData apiData = JsonUtil.jsonToObj(requestBody, OpenApiData.class);
            Assert.notNull(apiData, "请求数据不能为空.");
            // 验证时间戳和Nonce（在解密之前验证，避免无效请求消耗资源）
            Long timestamp = apiData.getTimestamp();
            String nonce = apiData.getNonce();
            Assert.notNull(timestamp, "Timestamp is empty.");
            Assert.hasText(nonce, "Nonce is empty.");
            Assert.isTrue(TimestampValidator.validate(timestamp, nonce), "时间戳或Nonce验证失败");

            // 解析加密请求
            RsaConfig rsaConfig = systemConfigService.getSystemConfig().getRsaConfig();
            // 判断是否为公网场景
            boolean isPublicNetwork = isPublicNetwork(request);
            String decryptedData = rsaManager.decrypt(rsaConfig, apiData, isPublicNetwork);
            Object json = JSON.parse(decryptedData);
            ServletWebRequest webRequest = getServletWebRequest(request, response, (String) json);
            ModelAndViewContainer mavContainer = new ModelAndViewContainer();
            mavContainer.addAllAttributes(RequestContextUtils.getInputFlashMap(request));
            Object result = invocableMethod.invokeForRequest(webRequest, mavContainer);
            // 加密返回
            if (result instanceof RestResult) {
                RestResult restResult = (RestResult) result;
                if (restResult.getData() != null && restResult.isSuccess()) {
                    restResult.setData(rsaManager.encrypt(rsaConfig, restResult.getData(), isPublicNetwork));
                }
            }
            return result;
        } catch (Exception e) {
            logger.error("OpenAPI 执行失败", e);
            return OpenApiResponse.fail(OpenApiErrorCode.INTERNAL_ERROR, "请求处理失败: " + e.getMessage());
        }
    }

    private static ServletWebRequest getServletWebRequest(HttpServletRequest request, HttpServletResponse response, String decryptedData) {
        DecryptRequestWrapper finalRequest = new DecryptRequestWrapper(request, decryptedData);
        return new ServletWebRequest(finalRequest, response);
    }

    @Override
    public void afterPropertiesSet() {
        // 初始化拦截mapping
        initHandlerMapping();
        // 添加解析器
        resolvers.addResolvers(requestMappingHandlerAdapter.getArgumentResolvers());
    }

    private void initHandlerMapping() {
        Set<String> parsePackage = new HashSet<>();
        parsePackage.add("/connector/");
        parsePackage.add("/mapping/");
        RequestMappingHandlerMapping mapping = applicationContext.getBean(RequestMappingHandlerMapping.class);
        // 获取url与类和方法的对应信息
        Map<RequestMappingInfo, HandlerMethod> map = mapping.getHandlerMethods();
        map.forEach((k, v) -> {
            PatternsRequestCondition condition = k.getPatternsCondition();
            assert condition != null;
            Object[] array = condition.getPatterns().toArray();
            boolean filter;
            for (String path : parsePackage) {
                filter = false;
                // 匹配解析包
                for (Object o : array) {
                    String patternPath = (String) o;
                    if (StringUtil.startsWith(patternPath, path)) {
                        handlers.compute(patternPath, (x, y) -> {
                            if (y == null) {
                                Object bean = applicationContext.getBean(v.getBeanType());
                                InvocableHandlerMethod invocableHandlerMethod = new InvocableHandlerMethod(bean, v.getMethod());
                                invocableHandlerMethod.setHandlerMethodArgumentResolvers(resolvers);
                                return invocableHandlerMethod;
                            }
                            return y;
                        });
                        // 记录该 path 允许的 HTTP 方法（空表示 @RequestMapping 未指定 method，允许任意）
                        Set<RequestMethod> methods = k.getMethodsCondition().getMethods();
                        handlerAllowedMethods.put(patternPath, methods.isEmpty() ? Collections.emptySet() : new HashSet<>(methods));
                        filter = true;
                        break;
                    }
                }
                if (filter) {
                    break;
                }
            }

        });
        parsePackage.clear();
    }

    /**
     * 同步接口
     * POST /openapi/data/sync
     *
     * @param request 请求对象
     * @return 同步结果
     */
    @PostMapping("/data/sync")
    public OpenApiResponse<Object> sync(HttpServletRequest request) {
        try {
//            String requestBody = readRequestBody(request);
//            boolean publicNetwork = isPublicNetwork(request);
//            RsaConfig rsaConfig = systemConfigService.getSystemConfig().getRsaConfig();
//            String decryptedData = rsaManager.decryptData(rsaConfig, requestBody, publicNetwork);
            // TODO 实现具体逻辑
            logger.info("同步数据：{}", true);
            return OpenApiResponse.success("同步数据", "success");
        } catch (Exception e) {
            logger.error("同步失败", e);
            return OpenApiResponse.fail(OpenApiErrorCode.INTERNAL_ERROR, "同步失败: " + e.getMessage());
        }
    }

    /**
     * 登录接口 - 获取Token
     * POST /openapi/auth/login
     *
     * @param requestBody 请求体（JSON格式，包含secret）
     * @return Token信息
     */
    @PostMapping("/auth/login")
    public OpenApiResponse<Map<String, String>> login(@RequestBody Map<String, String> requestBody) {
        try {
            String secret = requestBody.get("secret");
            if (StringUtil.isBlank(secret)) {
                return OpenApiResponse.fail(OpenApiErrorCode.BAD_REQUEST, "secret不能为空");
            }

            // 验证API密钥
            SystemConfig systemConfig = systemConfigService.getSystemConfig();
            if (!apiKeyManager.validate(systemConfig.getApiKeyConfig(), secret)) {
                logger.error("无效凭证 {}", secret);
                return OpenApiResponse.fail(OpenApiErrorCode.UNAUTHORIZED, "无效凭证");
            }

            // 获取JWT密钥（如果不存在会自动生成）
            Map<String, String> data = new HashMap<>();
            data.put("token", jwtSecretManager.generateToken()); // 生成Token
            data.put("expires", "7200"); // 2小时，单位：秒
            return OpenApiResponse.success("登录成功", data);
        } catch (Exception e) {
            logger.error("登录失败", e);
            return OpenApiResponse.fail(OpenApiErrorCode.INTERNAL_ERROR, "登录失败: " + e.getMessage());
        }
    }

    /**
     * 刷新Token接口
     * POST /openapi/auth/refresh
     *
     * @param request 请求对象（拦截器已解析token）
     * @return 新的Token信息
     */
    @PostMapping("/auth/refresh")
    public OpenApiResponse<Map<String, String>> refreshToken(HttpServletRequest request) {
        try {
            // 从请求头获取原Token
            String oldToken = request.getHeader("Authorization");
            if (oldToken != null && oldToken.startsWith("Bearer ")) {
                oldToken = oldToken.substring(7);
            }
            if (StringUtil.isBlank(oldToken)) {
                return OpenApiResponse.fail(OpenApiErrorCode.UNAUTHORIZED, "Token不能为空");
            }
            // 刷新Token
            String newToken = jwtSecretManager.refreshToken(oldToken);
            if (StringUtil.isBlank(newToken)) {
                return OpenApiResponse.fail(OpenApiErrorCode.BAD_REQUEST, "Token无效或已过期");
            }

            Map<String, String> data = new HashMap<>();
            data.put("token", newToken);
            data.put("expires", "7200");
            return OpenApiResponse.success("刷新Token成功", data);
        } catch (Exception e) {
            logger.error("刷新Token失败", e);
            return OpenApiResponse.fail(OpenApiErrorCode.INTERNAL_ERROR, "刷新Token失败: " + e.getMessage());
        }
    }

    /**
     * 将请求方法字符串转为 RequestMethod；不支持的方法返回 null
     */
    private RequestMethod parseRequestMethod(String method) {
        if (StringUtil.isBlank(method)) {
            return null;
        }
        try {
            return RequestMethod.valueOf(method.toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * 从请求中解析 /openapi/v1 之后的路径，作为内部 handler 的 lookup key（如 /connector/test）
     */
    private String getV1LookupPath(HttpServletRequest request) {
        String uri = urlPathHelper.getLookupPathForRequest(request);
        if (StringUtil.isBlank(uri)) {
            return null;
        }
        if (!uri.startsWith(OPENAPI_V1_PREFIX)) {
            return null;
        }
        String path = uri.substring(OPENAPI_V1_PREFIX.length());
        return path.startsWith("/") ? path : "/" + path;
    }

    /**
     * 读取请求体
     */
    private String readRequestBody(HttpServletRequest request) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = request.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }

    /**
     * 判断是否为公网场景
     */
    private boolean isPublicNetwork(HttpServletRequest request) {
        return "true".equalsIgnoreCase(request.getHeader(PUBLIC_NETWORK_HEADER));
    }

}
