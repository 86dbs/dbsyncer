/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.openapi;

import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.impl.ApiKeyManager;
import org.dbsyncer.biz.impl.JwtSecretManager;
import org.dbsyncer.common.model.JwtSecretVersion;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.web.model.OpenApiResponse;
import org.dbsyncer.web.security.JwtUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
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

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private RequestMappingHandlerAdapter requestMappingHandlerAdapter;

    @Resource
    private ApplicationContext applicationContext;

    private final Map<String, String> parsePackage = new HashMap<>();
    private final Map<String, InvocableHandlerMethod> handlers = new ConcurrentHashMap<>();
    private final HandlerMethodArgumentResolverComposite resolvers = new HandlerMethodArgumentResolverComposite();
    private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();

    @ResponseBody
    @RequestMapping("/api.json")
    public Object adapter(HttpServletRequest request, HttpServletResponse response) {
        try {
            InvocableHandlerMethod invocableMethod = handlers.get("/gateway/demo.json");
            // 模拟参数
            Map<String, Object> params = new HashMap<>();
            params.put("id", 1000L);
            params.put("version", "20201124");

            // 获取函数参数
            List<Object> providedArgs = new ArrayList<>();
            String[] parameterNames = parameterNameDiscoverer.getParameterNames(invocableMethod.getMethod());
            logger.info(Arrays.toString(parameterNames));
            if (!ObjectUtils.isEmpty(parameterNames)) {
                for (String parameterName : parameterNames) {
                    providedArgs.add(params.get(parameterName));
                }
            }

            ServletWebRequest webRequest = new ServletWebRequest(request, response);
            ModelAndViewContainer mavContainer = new ModelAndViewContainer();
            mavContainer.addAllAttributes(RequestContextUtils.getInputFlashMap(request));
            return invocableMethod.invokeForRequest(webRequest, mavContainer, providedArgs.toArray());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    @Override
    public void afterPropertiesSet() {
        // 初始化拦截mapping
        initHandlerMapping();
        // 添加解析器
        resolvers.addResolvers(requestMappingHandlerAdapter.getArgumentResolvers());
    }

    private void initHandlerMapping() {
        parsePackage.put("/openapi/", "");
        RequestMappingHandlerMapping mapping = applicationContext.getBean(RequestMappingHandlerMapping.class);
        // 获取url与类和方法的对应信息
        Map<RequestMappingInfo, HandlerMethod> map = mapping.getHandlerMethods();
        map.forEach((k, v) -> {
            PatternsRequestCondition condition = k.getPatternsCondition();
            assert condition != null;
            Object[] array = condition.getPatterns().toArray();
            boolean filter;
            for (Map.Entry<String, String> obj : parsePackage.entrySet()) {
                filter = false;
                // 匹配解析包
                for (Object o : array) {
                    if (StringUtil.startsWith((String) o, obj.getKey())) {
                        handlers.compute((String) o, (x, y) -> {
                            if (y == null) {
                                Object bean = applicationContext.getBean(v.getBeanType());
                                InvocableHandlerMethod invocableHandlerMethod = new InvocableHandlerMethod(bean, v.getMethod());
                                invocableHandlerMethod.setHandlerMethodArgumentResolvers(resolvers);
                                return invocableHandlerMethod;
                            }
                            return y;
                        });
                        filter = true;
                        break;
                    }
                }
                if (filter) {
                    break;
                }
            }

        });
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
                return OpenApiResponse.fail(500, "未开放API");
            }

            String secret = requestBody.get("secret");
            if (StringUtil.isBlank(secret)) {
                return OpenApiResponse.fail(400, "secret不能为空");
            }

            // 验证API密钥
            if (!apiKeyManager.validate(systemConfig.getApiKeyConfig(), secret)) {
                logger.error("无效凭证 {}", secret);
                return OpenApiResponse.fail(401, "无效凭证");
            }
            
            // 获取JWT密钥（如果不存在会自动生成）
            String jwtSecret = jwtSecretManager.getCurrentSecret();

            Map<String, String> data = new HashMap<>();
            data.put("token", JwtUtil.generateToken(jwtSecret)); // 生成Token
            data.put("expiresIn", "7200"); // 2小时，单位：秒
            return OpenApiResponse.success("登录成功", data);
        } catch (Exception e) {
            logger.error("登录失败", e);
            return OpenApiResponse.fail(500, "登录失败: " + e.getMessage());
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
            
            // 刷新Token（尝试使用当前密钥和上一个密钥）
            String newToken = null;

            // 获取JWT密钥（支持密钥轮换）
            List<JwtSecretVersion> jwtSecrets = jwtSecretManager.getReversedSecrets();
            for (JwtSecretVersion jwtSecret : jwtSecrets) {
                newToken = JwtUtil.refreshToken(oldToken, jwtSecret.getSecret());
                if (newToken != null) {
                    break;
                }
            }
            jwtSecrets.clear();
            if (newToken == null) {
                return OpenApiResponse.fail(400, "Token不在刷新时间窗口内");
            }
            
            Map<String, String> data = new HashMap<>();
            data.put("token", newToken);
            data.put("expiresIn", "7200");

            return OpenApiResponse.success("刷新Token成功", data);
        } catch (Exception e) {
            logger.error("刷新Token失败", e);
            return OpenApiResponse.fail(500, "刷新Token失败: " + e.getMessage());
        }
    }

}
