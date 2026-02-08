/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.openapi;

import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.impl.ApiKeyManager;
import org.dbsyncer.biz.impl.JwtSecretManager;
import org.dbsyncer.biz.impl.RsaManager;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.model.RsaConfig;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.manager.impl.PreloadTemplate;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.web.model.OpenApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.GetMapping;
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

    @Resource
    private PreloadTemplate preloadTemplate;

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
     * OpenAPI v1 统一入口，将 /openapi/v1/xxx 转发到内部 Controller 的 /xxx
     * <p>
     * 示例：<br>
     * - /openapi/v1/connector/test → 转发到 /connector/test<br>
     * - /openapi/v1/test → 转发到 /test
     * </p>
     */
    @RequestMapping(value = "/v1/**")
    public Object adapter(HttpServletRequest request, HttpServletResponse response) {
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
                    String allow = allowedMethods.stream().map(Enum::name).reduce((a, b) -> a + ", " + b).orElse("");
                    logger.warn("OpenAPI v1 请求方法不允许: path={}, method={}, allowed={}", lookupPath, request.getMethod(), allow);
                    return OpenApiResponse.fail(OpenApiErrorCode.METHOD_NOT_ALLOWED, request.getMethod() + "请求方法无效，只允许: " + allow);
                }
            }

            ServletWebRequest webRequest = new ServletWebRequest(request, response);
            ModelAndViewContainer mavContainer = new ModelAndViewContainer();
            mavContainer.addAllAttributes(RequestContextUtils.getInputFlashMap(request));
            // TODO 解密入参
//            // 5. 解析加密请求（如果是加密接口）
//            if (isEncryptedEndpoint(requestPath)) {
//                String requestBody = readRequestBody(request);
//                if (StringUtil.isBlank(requestBody)) {
//                    writeErrorResponse(response, OpenApiErrorCode.BAD_REQUEST, "请求体不能为空");
//                    return false;
//                }
//
//                try {
//                    // 先解析为OpenApiRequest格式（包含时间戳和nonce）
//                    OpenApiRequest encryptedRequest = JsonUtil.jsonToObj(requestBody, OpenApiRequest.class);
//
//                    // 验证时间戳和Nonce（在解密之前验证，避免无效请求消耗资源）
//                    if (encryptedRequest.getTimestamp() != null &&
//                        StringUtil.isNotBlank(encryptedRequest.getNonce())) {
//                        if (!TimestampValidator.validate(encryptedRequest.getTimestamp(), encryptedRequest.getNonce())) {
//                            writeErrorResponse(response, OpenApiErrorCode.BAD_REQUEST, "时间戳或Nonce验证失败");
//                            return false;
//                        }
//                    }
//
//                    // 判断是否为公网场景
//                    boolean isPublicNetwork = isPublicNetwork(request);
//
//                    // 解析加密请求（解密数据）
//                    String decryptedData = rsaManager.decryptedData(systemConfig.getRsaConfig(), requestBody, isPublicNetwork);
//
//                    // 将解密后的数据存储到request attribute中，供Controller使用
//                    request.setAttribute("decryptedData", decryptedData);
//                } catch (Exception e) {
//                    logger.error("解析加密请求失败", e);
//                    writeErrorResponse(response, OpenApiErrorCode.BAD_REQUEST, "解析加密请求失败: " + e.getMessage());
//                    return false;
//                }
//            }

            Object result = invocableMethod.invokeForRequest(webRequest, mavContainer);
            // 加密返回
            buildEncryptedRestResult(request, result);
            return result;
        } catch (Exception e) {
            logger.error("OpenAPI adapter 执行失败", e);
            return OpenApiResponse.fail(OpenApiErrorCode.INTERNAL_ERROR, "请求处理失败: " + e.getMessage());
        }
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
     * 查看文档接口
     * GET /openapi/api
     *
     * @param request 请求对象
     * @return Token信息
     */
    @GetMapping("/api")
    public OpenApiResponse<Object> api(HttpServletRequest request) {
        try {
            // TODO 实现具体逻辑
            logger.info("查看文档");
            return OpenApiResponse.success("查看文档", "success");
        } catch (Exception e) {
            logger.error("查看文档失败", e);
            return OpenApiResponse.fail(OpenApiErrorCode.INTERNAL_ERROR, "查看文档失败: " + e.getMessage());
        }
    }

    /**
     * TODO 单独放一个控制器 同步接口
     * POST /openapi/data/sync
     *
     * @param request 请求对象
     * @return Token信息
     */
    @PostMapping("/data/sync")
    public OpenApiResponse<Object> sync(HttpServletRequest request) {
        try {
            String requestBody = readRequestBody(request);
            boolean publicNetwork = isPublicNetwork(request);
            RsaConfig rsaConfig = systemConfigService.getSystemConfig().getRsaConfig();
            String decryptedData = rsaManager.decryptData(rsaConfig, requestBody, publicNetwork);
            // TODO 实现具体逻辑
            logger.info("同步数据：{}", decryptedData);
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
            SystemConfig systemConfig = systemConfigService.getSystemConfig();
            if (!systemConfig.isEnableOpenAPI() || systemConfig.getApiKeyConfig() == null) {
                return OpenApiResponse.fail(OpenApiErrorCode.NOT_FOUND, "未开放API");
            }
            if (!preloadTemplate.isPreloadCompleted()) {
                return OpenApiResponse.fail(OpenApiErrorCode.SERVICE_UNAVAILABLE, "服务暂不可用");
            }

            String secret = requestBody.get("secret");
            if (StringUtil.isBlank(secret)) {
                return OpenApiResponse.fail(OpenApiErrorCode.BAD_REQUEST, "secret不能为空");
            }

            // 验证API密钥
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
            if (!preloadTemplate.isPreloadCompleted()) {
                return OpenApiResponse.fail(OpenApiErrorCode.SERVICE_UNAVAILABLE, "服务暂不可用");
            }
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

    private void buildEncryptedRestResult(HttpServletRequest request, Object result) {
        if (!(result instanceof RestResult)) {
            return;
        }
        RestResult restResult = (RestResult) result;
        if (restResult.getData() == null || !restResult.isSuccess()) {
            return;
        }
        boolean publicNetwork = isPublicNetwork(request);
        RsaConfig rsaConfig = systemConfigService.getSystemConfig().getRsaConfig();
        restResult.setData(rsaManager.encryptData(rsaConfig, restResult.getData(), publicNetwork));
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
