/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.web.controller.openapi;

import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.DateFormatUtil;
import org.dbsyncer.common.util.RandomUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.sdk.spi.TaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.stereotype.Controller;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OpenAPI
 *
 * @Version 1.0.0
 * @Author AE86
 * @Date 2023-11-16 01:38
 */
@Controller
@RequestMapping("/gateway")
public class OpenApiController implements InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private RequestMappingHandlerAdapter requestMappingHandlerAdapter;

    @Resource
    private ApplicationContext applicationContext;

    private final Map<String, String> parsePackage = new HashMap<>();
    private final Map<String, InvocableHandlerMethod> handlers = new ConcurrentHashMap<>();
    private final HandlerMethodArgumentResolverComposite resolvers = new HandlerMethodArgumentResolverComposite();
    private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();

    @ResponseBody
    @RequestMapping("/demo.json")
    public RestResult demo(Long id, String version) {
        logger.info("id:{},version:{}", id, version);
        return RestResult.restSuccess(RandomUtil.nextInt(1, 100));
    }

    @Resource
    TaskService taskService;

    @ResponseBody
    @RequestMapping("/testAddTask")
    public RestResult testAddTask() {

        Map<String, String> param = new HashMap<>();
        param.put("name", "订单数据同步任务");
        param.put("status", "1");
        param.put("type", "FULL_SYNC");
        param.put("json", "{\"sourceConnectorId\":\"conn_mysql_source_001\",\"sourceDatabaseName\":\"order_db\",\"targetConnectorId\":\"conn_postgres_target_001\",\"targetDatabaseName\":\"order_db_copy\",\"trigger\":\"timing\",\"cron\":\"0 0 1 * * ?\",\"autoMatchTable\":false,\"verification\":true,\"correction\":false,\"tableStructure\":true,\"rowData\":true,\"index\":false,\"triggerFlag\":false,\"function\":false,\"storedProcedure\":false,\"tableMappings\":[{\"sourceTable\":{\"name\":\"order_main\",\"schema\":\"public\",\"columns\":[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"order_no\",\"type\":\"VARCHAR(50)\"},{\"name\":\"create_time\",\"type\":\"DATETIME\"}]},\"targetTable\":{\"name\":\"order_main_backup\",\"schema\":\"public\",\"columns\":[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"order_no\",\"type\":\"VARCHAR(50)\"},{\"name\":\"create_time\",\"type\":\"TIMESTAMP\"}]},\"fieldMapping\":[{\"sourceField\":\"id\",\"targetField\":\"id\",\"typeHandler\":\"DEFAULT\"},{\"sourceField\":\"order_no\",\"targetField\":\"order_no\",\"typeHandler\":\"DEFAULT\"},{\"sourceField\":\"create_time\",\"targetField\":\"create_time\",\"typeHandler\":\"DATETIME_TO_TIMESTAMP\"}]},{\"sourceTable\":{\"name\":\"order_item\",\"schema\":\"public\",\"columns\":[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"order_id\",\"type\":\"INT\"},{\"name\":\"product_name\",\"type\":\"VARCHAR(100)\"}]},\"targetTable\":{\"name\":\"order_item_backup\",\"schema\":\"public\",\"columns\":[{\"name\":\"id\",\"type\":\"INT\"},{\"name\":\"order_id\",\"type\":\"INT\"},{\"name\":\"product_name\",\"type\":\"VARCHAR(100)\"}]},\"fieldMapping\":[{\"sourceField\":\"id\",\"targetField\":\"id\",\"typeHandler\":\"DEFAULT\"},{\"sourceField\":\"order_id\",\"targetField\":\"order_id\",\"typeHandler\":\"DEFAULT\"},{\"sourceField\":\"product_name\",\"targetField\":\"product_name\",\"typeHandler\":\"DEFAULT\"}]}]}");
        param.put("createTime", DateFormatUtil.dateToString(new Date()));
        param.put("updateTime", DateFormatUtil.dateToString(new Date()));
        taskService.add(param);

        return RestResult.restSuccess("新增成功");
    }



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
        parsePackage.put("/gateway/", "");
        RequestMappingHandlerMapping mapping = applicationContext.getBean(RequestMappingHandlerMapping.class);
        // 获取url与类和方法的对应信息
        Map<RequestMappingInfo, HandlerMethod> map = mapping.getHandlerMethods();
        map.forEach((k, v) -> {
            PatternsRequestCondition condition = k.getPatternsCondition();
            Object[] array = condition.getPatterns().toArray();
            int length = array.length;
            boolean filter;
            for (Map.Entry<String, String> obj : parsePackage.entrySet()) {
                filter = false;
                // 匹配解析包
                for (int i = 0; i < length; i++) {
                    if (StringUtil.startsWith((String) array[i], obj.getKey())) {
                        handlers.compute((String) array[i], (x, y) -> {
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

}