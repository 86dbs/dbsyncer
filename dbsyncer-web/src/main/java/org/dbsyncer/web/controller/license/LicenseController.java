/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.web.controller.license;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.dbsyncer.biz.UserConfigService;
import org.dbsyncer.biz.vo.ProductInfoVo;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.model.UserInfo;
import org.dbsyncer.sdk.model.Product;
import org.dbsyncer.sdk.model.ProductInfo;
import org.dbsyncer.sdk.spi.LicenseService;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

@Controller
@RequestMapping("/license")
public class LicenseController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private LogService logService;

    @Resource
    private LicenseService licenseService;

    @Resource
    private UserConfigService userConfigService;

    public static final Integer SUCCESS = 200;
    public static final String STATUS = "status";
    public static final String DATA = "data";
    public static final String MSG = "msg";
    public static final String SERVER_ADDRESS = "http://117.72.11.38:8989/api/license/create";

    @RequestMapping("")
    public String index(ModelMap model) {
        model.put("key", licenseService.getKey());
        model.put("userInfo", getUserInfo());
        model.put("productInfo", licenseService.getProductInfo());
        return "license/license";
    }

    @PostMapping(value = "/upload")
    @ResponseBody
    public RestResult upload(MultipartFile[] files) {
        try {
            if (files != null && files[0] != null) {
                String filename = "license";
                File dest = new File(licenseService.getLicensePath() + filename);
                FileUtils.deleteQuietly(dest);
                FileUtils.copyInputStreamToFile(files[0].getInputStream(), dest);
                licenseService.updateLicense();
                logger.info("{}:{}", LogType.UserLog.UPLOAD_LICENSE_FILE.getMessage(), filename);
                logService.log(LogType.UserLog.UPLOAD_LICENSE_FILE);
            }
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logService.log(LogType.UserLog.UPLOAD_LICENSE_FILE_ERROR);
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/activate")
    @ResponseBody
    public RestResult activate(HttpServletRequest request) {
        try {
            String content = getLicenseContent(getParams(request));
            if (StringUtil.isNotBlank(content)) {
                String filename = "license";
                File dest = new File(licenseService.getLicensePath() + filename);
                FileUtils.writeStringToFile(dest, content, Charset.defaultCharset());
                licenseService.updateLicense();
                logger.info("{}:{}", LogType.UserLog.ACTIVATE_FREE_LICENSE_FILE.getMessage(), filename);
                logService.log(LogType.UserLog.ACTIVATE_FREE_LICENSE_FILE);
            }
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logService.log(LogType.UserLog.ACTIVATE_FREE_LICENSE_FILE_ERROR);
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/remove")
    @ResponseBody
    public RestResult remove() {
        try {
            String filename = "license";
            File dest = new File(licenseService.getLicensePath() + filename);
            FileUtils.deleteQuietly(dest);
            licenseService.updateLicense();
            logger.info("{}:{}", LogType.UserLog.DELETE_LICENSE_FILE.getMessage(), filename);
            logService.log(LogType.UserLog.DELETE_LICENSE_FILE);
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logService.log(LogType.UserLog.DELETE_LICENSE_FILE_ERROR);
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @GetMapping("/query.json")
    @ResponseBody
    public RestResult query() {
        ProductInfoVo infoVo = new ProductInfoVo();
        if (StringUtil.isNotBlank(licenseService.getKey())) {
            infoVo.setKey(licenseService.getKey());
            ProductInfo productInfo = licenseService.getProductInfo();
            if (productInfo != null && !CollectionUtils.isEmpty(productInfo.getProducts())) {
                Optional<Product> first = productInfo.getProducts().stream().min(Comparator.comparing(Product::getEffectiveTime));
                if (first.isPresent()) {
                    infoVo.setEffectiveTime(first.get().getEffectiveTime());
                    formatEffectiveTimeContent(infoVo);
                }
            }
        }
        return RestResult.restSuccess(infoVo);
    }

    private void formatEffectiveTimeContent(ProductInfoVo infoVo) {
        LocalDateTime startDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(infoVo.getCurrentTime()), ZoneId.systemDefault());
        LocalDateTime endDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(infoVo.getEffectiveTime()), ZoneId.systemDefault());
        Period period = Period.between(startDateTime.toLocalDate(), endDateTime.toLocalDate());
        Duration duration = Duration.between(startDateTime, endDateTime);
        long years = Math.max(period.getYears(), 0);
        long months = Math.max(period.getMonths(), 0);
        if (years > 0) {
            infoVo.setEffectiveContent(String.format("还剩%d年%d个月", years, months));
            return;
        }
        long days = Math.max(period.getDays(), 0);
        if (months > 0) {
            infoVo.setEffectiveContent(String.format("还剩%d个月%d天", months, days));
            return;
        }
        long hours = Math.max(duration.toHours() % 24, 0);
        infoVo.setEffectiveContent(String.format("还剩%d天%d小时", days, hours));
    }

    private String getLicenseContent(Map<String, String> params) throws IOException {
        ProductInfo info = JsonUtil.jsonToObj(JsonUtil.objToJson(params), ProductInfo.class);
        UserInfo userInfo = getUserInfo();
        Assert.notNull(userInfo, "会话过期，请重新登录");
        info.setLicenseKey(licenseService.getKey());
        info.setOsName(System.getProperty("os.name"));
        info.setPhone(StringUtil.isNotBlank(info.getPhone()) ? info.getPhone() : userInfo.getPhone());
        info.setEmail(StringUtil.isNotBlank(info.getEmail()) ? info.getEmail() : userInfo.getEmail());
        return invoke(info);
    }

    public String invoke(ProductInfo info) throws IOException {
        String data = URLEncoder.encode(JsonUtil.objToJson(info), "UTF-8");
        StringEntity se = new StringEntity(data);
        se.setContentEncoding("UTF-8");
        se.setContentType("application/json");
        HttpPost httpPost = new HttpPost(SERVER_ADDRESS);
        httpPost.setEntity(se);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            CloseableHttpResponse response = httpClient.execute(httpPost);
            if (response.getStatusLine().getStatusCode() == SUCCESS) {
                Map<String, String> result = JsonUtil.jsonToObj(EntityUtils.toString(response.getEntity()), Map.class);
                if (result.containsKey(DATA)) {
                    String status = String.valueOf(result.get(STATUS));
                    if (Integer.parseInt(status) == SUCCESS) {
                        return result.get(DATA);
                    }
                }
                throw new IllegalArgumentException(result.get(MSG));
            }
        } catch (HttpHostConnectException e) {
            throw new IllegalArgumentException("网络连接异常，无法激活");
        }
        throw new IllegalArgumentException("授权服务地址异常，无法激活");
    }

    private UserInfo getUserInfo() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return userConfigService.getUserInfo(authentication.getName());
    }

}