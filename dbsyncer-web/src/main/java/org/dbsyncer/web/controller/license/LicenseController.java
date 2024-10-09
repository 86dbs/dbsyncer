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
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.common.config.AppConfig;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.model.UserInfo;
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
import java.util.Map;

@Controller
@RequestMapping("/license")
public class LicenseController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private LogService logService;

    @Resource
    private LicenseService licenseService;

    @Resource
    private AppConfig appConfig;

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
        model.put("company", appConfig.getCompany());
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
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping(value = "/activate")
    @ResponseBody
    public RestResult activate(HttpServletRequest request) {
        try {
            ProductInfo productInfo = licenseService.getProductInfo();
            if (productInfo == null || (productInfo != null && CollectionUtils.isEmpty(productInfo.getProducts()))) {
                String content = getLicenseContent(getParams(request));
                if (StringUtil.isNotBlank(content)) {
                    String filename = "license";
                    File dest = new File(licenseService.getLicensePath() + filename);
                    FileUtils.writeStringToFile(dest, content, Charset.defaultCharset());
                    licenseService.updateLicense();
                    logger.info("{}:{}", LogType.UserLog.ACTIVATE_FREE_LICENSE_FILE.getMessage(), filename);
                    logService.log(LogType.UserLog.ACTIVATE_FREE_LICENSE_FILE);
                }
            }
            return RestResult.restSuccess("ok");
        } catch (Exception e) {
            logService.log(LogType.UserLog.ACTIVATE_FREE_LICENSE_FILE_ERROR);
            logger.error(e.getLocalizedMessage(), e.getClass());
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
            logger.error(e.getLocalizedMessage(), e.getClass());
            return RestResult.restFail(e.getMessage());
        }
    }

    private String getLicenseContent(Map<String, String> params) throws IOException {
        ProductInfo info = JsonUtil.jsonToObj(JsonUtil.objToJson(params), ProductInfo.class);
        UserInfo userInfo = getUserInfo();
        Assert.notNull(userInfo, "会话过期，请重新登录");
        info.setLicenseKey(licenseService.getKey());
        info.setCompany(StringUtil.isNotBlank(info.getCompany()) ? info.getCompany() : appConfig.getCompany());
        info.setOwner(StringUtil.isNotBlank(info.getOwner()) ? info.getOwner() : userInfo.getNickname());
        info.setPhone(StringUtil.isNotBlank(info.getPhone()) ? info.getPhone() : userInfo.getPhone());
        info.setMail(StringUtil.isNotBlank(info.getMail()) ? info.getMail() : userInfo.getMail());
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