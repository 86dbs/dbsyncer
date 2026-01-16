/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.controller.openapi;

import org.dbsyncer.biz.ConnectorService;
import org.dbsyncer.biz.SystemConfigService;
import org.dbsyncer.biz.vo.ConnectorVo;
import org.dbsyncer.web.model.OpenApiResponse;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.SystemConfig;
import org.dbsyncer.biz.impl.AppCredentialManager;
import org.dbsyncer.biz.impl.JwtSecretManager;
import org.dbsyncer.web.security.JwtUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.*;
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
 * 提供外部业务系统集成接口
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
    private ConnectorService connectorService;

    @Resource
    private JwtSecretManager jwtSecretManager;

    @Resource
    private AppCredentialManager appCredentialManager;

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
     * @param requestBody 请求体（JSON格式，包含appId和appSecret）
     * @return Token信息
     */
    @PostMapping("/auth/login")
    public OpenApiResponse<Map<String, String>> login(@RequestBody Map<String, String> requestBody) {
        try {
            String appId = requestBody.get("appId");
            String appSecret = requestBody.get("appSecret");
            
            if (StringUtil.isBlank(appId) || StringUtil.isBlank(appSecret)) {
                return OpenApiResponse.fail(400, "appId和appSecret不能为空");
            }
            
            // 验证appId和appSecret
            if (!appCredentialManager.validateCredential(appId, appSecret)) {
                logger.warn("业务系统 {} 凭证验证失败", appId);
                return OpenApiResponse.fail(401, "appId或appSecret错误");
            }
            
            // 获取系统配置中的RSA配置（用于生成JWT密钥）
            SystemConfig systemConfig = systemConfigService.getSystemConfig();
            if (systemConfig == null || !systemConfig.isEnableRsaConfig() || systemConfig.getRsaConfig() == null) {
                return OpenApiResponse.fail(500, "RSA配置未启用或不存在");
            }
            
            // 获取JWT密钥（如果不存在会自动生成）
            String jwtSecret = jwtSecretManager.getCurrentSecret();
            
            // 生成Token
            String token = JwtUtil.generateToken(appId, jwtSecret);
            
            Map<String, String> data = new HashMap<>();
            data.put("token", token);
            data.put("expiresIn", "7200"); // 2小时，单位：秒
            
            logger.info("业务系统 {} 登录成功", appId);
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
            JwtUtil.TokenInfo tokenInfo = (JwtUtil.TokenInfo) request.getAttribute("tokenInfo");
            if (tokenInfo == null) {
                return OpenApiResponse.fail(401, "Token信息不存在");
            }
            
            // 获取JWT密钥（支持密钥轮换）
            String[] jwtSecrets = jwtSecretManager.getSecretsForVerification();
            
            // 从请求头获取原Token
            String oldToken = request.getHeader("Authorization");
            if (oldToken != null && oldToken.startsWith("Bearer ")) {
                oldToken = oldToken.substring(7);
            }
            
            // 刷新Token（尝试使用当前密钥和上一个密钥）
            String newToken = null;
            for (String jwtSecret : jwtSecrets) {
                newToken = JwtUtil.refreshToken(oldToken, jwtSecret);
                if (newToken != null) {
                    break;
                }
            }
            if (newToken == null) {
                return OpenApiResponse.fail(400, "Token不在刷新时间窗口内");
            }
            
            Map<String, String> data = new HashMap<>();
            data.put("token", newToken);
            data.put("expiresIn", "7200");
            
            logger.info("业务系统 {} 刷新Token成功", tokenInfo.getAppId());
            return OpenApiResponse.success("刷新Token成功", data);
        } catch (Exception e) {
            logger.error("刷新Token失败", e);
            return OpenApiResponse.fail(500, "刷新Token失败: " + e.getMessage());
        }
    }

    /**
     * 获取连接器列表
     * GET /openapi/connector/list
     * 
     * @param request 请求对象
     * @return 连接器列表
     */
    @GetMapping("/connector/list")
    public OpenApiResponse<List<ConnectorVo>> getConnectorList(HttpServletRequest request) {
        try {
            String appId = (String) request.getAttribute("appId");
            logger.info("业务系统 {} 获取连接器列表", appId);
            
            // 获取所有连接器
            List<ConnectorVo> connectors = connectorService.getConnectorAll();
            
            return OpenApiResponse.success(connectors);
        } catch (Exception e) {
            logger.error("获取连接器列表失败", e);
            return OpenApiResponse.fail(500, "获取连接器列表失败: " + e.getMessage());
        }
    }

    /**
     * 获取连接器详情
     * GET /openapi/connector/{id}
     * 
     * @param id 连接器ID
     * @param request 请求对象
     * @return 连接器详情
     */
    @GetMapping("/connector/{id}")
    public OpenApiResponse<Connector> getConnector(@PathVariable String id, HttpServletRequest request) {
        try {
            String appId = (String) request.getAttribute("appId");
            logger.info("业务系统 {} 获取连接器详情: {}", appId, id);
            
            Connector connector = connectorService.getConnector(id);
            if (connector == null) {
                return OpenApiResponse.fail(404, "连接器不存在");
            }
            
            return OpenApiResponse.success(connector);
        } catch (Exception e) {
            logger.error("获取连接器详情失败", e);
            return OpenApiResponse.fail(500, "获取连接器详情失败: " + e.getMessage());
        }
    }

    /**
     * 创建连接器
     * POST /openapi/connector/create
     * 
     * @param request 请求对象（拦截器已解密数据）
     * @return 创建结果
     */
    @PostMapping("/connector/create")
    public OpenApiResponse<Map<String, String>> createConnector(HttpServletRequest request) {
        try {
            String appId = (String) request.getAttribute("appId");
            String decryptedData = (String) request.getAttribute("decryptedData");
            
            if (StringUtil.isBlank(decryptedData)) {
                return OpenApiResponse.fail(400, "请求数据不能为空");
            }
            
            // 解析连接器配置
            @SuppressWarnings("unchecked")
            Map<String, String> params = (Map<String, String>) JsonUtil.jsonToObj(decryptedData, Map.class);
            
            // 创建连接器
            String connectorId = connectorService.add(params);
            
            Map<String, String> data = new HashMap<>();
            data.put("connectorId", connectorId);
            
            logger.info("业务系统 {} 创建连接器成功: {}", appId, connectorId);
            return OpenApiResponse.success("创建连接器成功", data);
        } catch (Exception e) {
            logger.error("创建连接器失败", e);
            return OpenApiResponse.fail(500, "创建连接器失败: " + e.getMessage());
        }
    }

    /**
     * 更新连接器
     * POST /openapi/connector/update
     * 
     * @param request 请求对象（拦截器已解密数据）
     * @return 更新结果
     */
    @PostMapping("/connector/update")
    public OpenApiResponse<String> updateConnector(HttpServletRequest request) {
        try {
            String appId = (String) request.getAttribute("appId");
            String decryptedData = (String) request.getAttribute("decryptedData");
            
            if (StringUtil.isBlank(decryptedData)) {
                return OpenApiResponse.fail(400, "请求数据不能为空");
            }
            
            // 解析连接器配置
            @SuppressWarnings("unchecked")
            Map<String, String> params = (Map<String, String>) JsonUtil.jsonToObj(decryptedData, Map.class);
            
            // 更新连接器
            String result = connectorService.edit(params);
            
            logger.info("业务系统 {} 更新连接器成功: {}", appId, result);
            return OpenApiResponse.success(result);
        } catch (Exception e) {
            logger.error("更新连接器失败", e);
            return OpenApiResponse.fail(500, "更新连接器失败: " + e.getMessage());
        }
    }

    /**
     * 删除连接器
     * POST /openapi/connector/delete
     * 
     * @param request 请求对象（拦截器已解密数据）
     * @return 删除结果
     */
    @PostMapping("/connector/delete")
    public OpenApiResponse<String> deleteConnector(HttpServletRequest request) {
        try {
            String appId = (String) request.getAttribute("appId");
            String decryptedData = (String) request.getAttribute("decryptedData");
            
            if (StringUtil.isBlank(decryptedData)) {
                return OpenApiResponse.fail(400, "请求数据不能为空");
            }
            
            // 解析连接器ID
            Map<String, String> params = JsonUtil.jsonToObj(decryptedData, Map.class);
            String connectorId = params.get("id");
            
            if (StringUtil.isBlank(connectorId)) {
                return OpenApiResponse.fail(400, "连接器ID不能为空");
            }
            
            // 删除连接器
            String result = connectorService.remove(connectorId);
            
            logger.info("业务系统 {} 删除连接器成功: {}", appId, connectorId);
            return OpenApiResponse.success(result);
        } catch (Exception e) {
            logger.error("删除连接器失败", e);
            return OpenApiResponse.fail(500, "删除连接器失败: " + e.getMessage());
        }
    }

    /**
     * 数据同步接口 - 业务系统同步数据到DBSyncer
     * POST /openapi/data/sync
     * 
     * @param request 请求对象（拦截器已解密数据）
     * @return 同步结果
     */
    @PostMapping("/data/sync")
    public OpenApiResponse<Map<String, Object>> syncData(HttpServletRequest request) {
        try {
            String appId = (String) request.getAttribute("appId");
            String decryptedData = (String) request.getAttribute("decryptedData");
            
            if (StringUtil.isBlank(decryptedData)) {
                return OpenApiResponse.fail(400, "同步数据不能为空");
            }
            
            // 解析同步数据
            @SuppressWarnings("unchecked")
            Map<String, Object> syncData = (Map<String, Object>) JsonUtil.jsonToObj(decryptedData, Map.class);
            
            // TODO: 实现数据同步逻辑
            // 这里需要根据业务需求实现具体的数据同步处理
            // syncData包含同步的数据内容，例如：connectorId, tableName, data等
            logger.debug("接收到同步数据: {}", syncData);
            
            Map<String, Object> result = new HashMap<>();
            result.put("syncId", System.currentTimeMillis());
            result.put("status", "success");
            result.put("message", "数据同步成功");
            
            logger.info("业务系统 {} 同步数据成功", appId);
            return OpenApiResponse.success("数据同步成功", result);
        } catch (Exception e) {
            logger.error("数据同步失败", e);
            return OpenApiResponse.fail(500, "数据同步失败: " + e.getMessage());
        }
    }

    /**
     * 获取数据接口 - DBSyncer调用业务系统获取数据
     * POST /openapi/data/fetch
     * 
     * @param request 请求对象（拦截器已解密数据）
     * @return 获取的数据
     */
    @PostMapping("/data/fetch")
    public OpenApiResponse<Object> fetchData(HttpServletRequest request) {
        try {
            String appId = (String) request.getAttribute("appId");
            String decryptedData = (String) request.getAttribute("decryptedData");
            
            if (StringUtil.isBlank(decryptedData)) {
                return OpenApiResponse.fail(400, "请求参数不能为空");
            }
            
            // 解析请求参数
            @SuppressWarnings("unchecked")
            Map<String, Object> params = (Map<String, Object>) JsonUtil.jsonToObj(decryptedData, Map.class);
            
            // TODO: 实现数据获取逻辑
            // 这里需要根据业务需求实现具体的数据获取处理
            // 例如：根据连接器ID、表名等条件获取数据
            
            logger.info("业务系统 {} 获取数据", appId);
            return OpenApiResponse.success("数据获取成功", params);
        } catch (Exception e) {
            logger.error("获取数据失败", e);
            return OpenApiResponse.fail(500, "获取数据失败: " + e.getMessage());
        }
    }

}
