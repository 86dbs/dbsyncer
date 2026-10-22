package org.dbsyncer.web.controller.user;

import org.dbsyncer.biz.PermissionService;
import org.dbsyncer.biz.RoleService;
import org.dbsyncer.biz.UserConfigService;
import org.dbsyncer.biz.vo.RestResult;
import org.dbsyncer.biz.vo.UserInfoVO;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.web.controller.BaseController;
import org.dbsyncer.web.security.LoginService;
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

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
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

    @Resource
    private UserConfigService userConfigService;

    @Resource
    private PermissionService permissionService;

    @Resource
    private RoleService roleService;

    @Resource
    private LoginService loginService;

    @Resource
    private LogService logService;

    @RequestMapping("")
    public String index(ModelMap model) {
        model.put("currentUser", getUserInfoVo());
        List<UserInfoVO> users = userConfigService.getUserInfoAll(getUserName());
        if (users != null) {
            for (UserInfoVO u : users) {
                if (u != null) {
                    u.setLocked(loginService.isLocked(u.getUsername()));
                }
            }
        }
        model.put("users", users);
        return "user/list";
    }

    @GetMapping("/page/add")
    public String pageAdd(ModelMap model) {
        requirePermission("user:edit");
        model.put("roles", roleService.listRoles());
        return "user/add";
    }

    @GetMapping("/page/edit")
    public String pageEdit(ModelMap model, String username) {
        String currentUserName = getUserName();
        username = StringUtil.isBlank(username) ? currentUserName : username;
        model.put(UserConfigService.CURRENT_USER_NAME, currentUserName);
        model.put("currentUser", userConfigService.getUserInfoVo(currentUserName, username));
        model.put("roles", roleService.listRoles());
        model.put("canAssignRole", permissionService.hasPermission(currentUserName, "user:edit"));
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

    /**
     * 提前解除登录失败锁定。
     */
    @PostMapping("/unlock")
    @ResponseBody
    public RestResult unlock(HttpServletRequest request) {
        try {
            requirePermission("user:edit");
            Map<String, String> params = getParamsWithUserName(request);
            String username = params.get("username");
            Assert.hasText(username, "用户名不能为空");
            loginService.unlock(username);
            logService.log(LogType.UserLog.UPDATE,
                    String.format("[%s]解锁账号[%s]", getUserName(), username));
            return RestResult.restSuccess("解锁成功");
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
     * 获取登录用户信息（含权限码，供前端显隐）。
     */
    private UserInfoVO getUserInfoVo() {
        String currentUserName = getUserName();
        UserInfoVO vo = userConfigService.getUserInfoVo(currentUserName, currentUserName);
        if (vo != null) {
            vo.setAdmin(permissionService.isAdmin(currentUserName));
            vo.setPermissions(new ArrayList<>(permissionService.listPermissionCodes(currentUserName)));
        }
        return vo;
    }

    private String getUserName() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String username = authentication.getName();
        Assert.hasText(username, "无法获取登录用户.");
        return username;
    }

}
