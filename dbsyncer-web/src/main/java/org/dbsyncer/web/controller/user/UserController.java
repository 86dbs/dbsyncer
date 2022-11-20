package org.dbsyncer.web.controller.user;

import org.dbsyncer.biz.UserConfigService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.biz.vo.UserInfoVo;
import org.dbsyncer.web.controller.BaseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * 用户信息管理
 *
 * @author AE86
 * @ClassName: UserController
 * @date: 2017年7月7日 上午10:03:33
 */
@Controller
@RequestMapping(value = "/user")
public class UserController extends BaseController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private UserConfigService userConfigService;

    @RequestMapping("")
    public String index(ModelMap model) {
        model.put("currentUser", getUserInfoVo());
        model.put("users", userConfigService.getUserInfoAll(getUserName()));
        return "user/user";
    }

    @GetMapping("/page/add")
    public String pageAdd(ModelMap model) {
        return "user/add";
    }

    @GetMapping("/page/edit")
    public String pageEdit(ModelMap model, String username) {
        String currentUserName = getUserName();
        model.put(UserConfigService.CURRENT_USER_NAME, currentUserName);
        model.put("currentUser", userConfigService.getUserInfoVo(currentUserName, username));
        return "user/edit";
    }

    @GetMapping("/getUserInfo.json")
    @ResponseBody
    public RestResult getUserInfo() {
        return RestResult.restSuccess(getUserInfoVo());
    }

    @RequestMapping(value = "/add")
    @ResponseBody
    public RestResult add(HttpServletRequest request) {
        try {
            Map<String, String> params = getParamsWithUserName(request);
            return RestResult.restSuccess(userConfigService.add(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @RequestMapping(value = "/edit")
    @ResponseBody
    public RestResult edit(HttpServletRequest request) {
        try {
            Map<String, String> params = getParamsWithUserName(request);
            return RestResult.restSuccess(userConfigService.edit(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    @PostMapping("/remove")
    @ResponseBody
    public RestResult remove(HttpServletRequest request) {
        try {
            Map<String, String> params = getParamsWithUserName(request);
            return RestResult.restSuccess(userConfigService.remove(params));
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            return RestResult.restFail(e.getMessage());
        }
    }

    private Map<String, String> getParamsWithUserName(HttpServletRequest request) {
        Map<String, String> params = getParams(request);
        params.put(UserConfigService.CURRENT_USER_NAME, getUserName());
        return params;
    }

    /**
     * 获取登录用户信息
     *
     * @return
     */
    private UserInfoVo getUserInfoVo() {
        String currentUserName = getUserName();
        return userConfigService.getUserInfoVo(currentUserName, currentUserName);
    }

    private String getUserName() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String username = authentication.getName();
        Assert.hasText(username, "无法获取登录用户.");
        return username;
    }


}
