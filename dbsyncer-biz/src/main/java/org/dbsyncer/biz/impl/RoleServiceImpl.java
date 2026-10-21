/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.BizException;
import org.dbsyncer.biz.PermissionChecker;
import org.dbsyncer.biz.RoleService;
import org.dbsyncer.biz.UserConfigService;
import org.dbsyncer.biz.vo.PermissionVO;
import org.dbsyncer.biz.vo.RoleVO;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.LogService;
import org.dbsyncer.parser.LogType;
import org.dbsyncer.parser.RbacProfile;
import org.dbsyncer.parser.model.Permission;
import org.dbsyncer.parser.model.Role;
import org.dbsyncer.parser.model.RolePermission;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link RoleService} 实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
@Service
public class RoleServiceImpl implements RoleService {

    private static final String PERM_USER_EDIT = "user:edit";

    private static final Map<String, String> MODULE_NAMES;

    static {
        Map<String, String> names = new java.util.LinkedHashMap<>();
        names.put("user", "用户");
        names.put("connector", "连接器");
        names.put("mapping", "同步任务");
        names.put("notice", "告警通知");
        names.put("license", "授权许可");
        names.put("system", "系统配置");
        MODULE_NAMES = Collections.unmodifiableMap(names);
    }

    @Resource
    private RbacProfile rbacProfile;

    @Resource
    private PermissionChecker permissionChecker;

    @Resource
    private LogService logService;

    @Override
    public List<RoleVO> listRoles() {
        List<Role> roles = rbacProfile.listRoles();
        if (CollectionUtils.isEmpty(roles)) {
            return Collections.emptyList();
        }
        List<RoleVO> result = new ArrayList<>(roles.size());
        for (Role role : roles) {
            if (role == null) {
                continue;
            }
            result.add(toRoleVO(role, false));
        }
        result.sort(Comparator.comparing(RoleVO::getCode, Comparator.nullsLast(String::compareTo)));
        return result;
    }

    @Override
    public RoleVO getRole(String roleId) {
        Assert.hasText(roleId, "角色ID不能为空");
        Role role = rbacProfile.getRoleById(roleId);
        Assert.notNull(role, "角色不存在");
        return toRoleVO(role, true);
    }

    @Override
    public List<PermissionVO> listPermissions(String roleId) {
        List<Permission> permissions = rbacProfile.listPermissions();
        if (CollectionUtils.isEmpty(permissions)) {
            return Collections.emptyList();
        }
        Set<String> grantedIds = Collections.emptySet();
        if (StringUtil.isNotBlank(roleId)) {
            List<RolePermission> rels = rbacProfile.listRolePermissionsByRoleId(roleId);
            grantedIds = new HashSet<>();
            if (!CollectionUtils.isEmpty(rels)) {
                for (RolePermission rel : rels) {
                    if (rel != null && StringUtil.isNotBlank(rel.getPermissionId())) {
                        grantedIds.add(rel.getPermissionId());
                    }
                }
            }
        }
        List<PermissionVO> result = new ArrayList<>(permissions.size());
        for (Permission permission : permissions) {
            if (permission == null) {
                continue;
            }
            PermissionVO vo = new PermissionVO();
            BeanUtils.copyProperties(permission, vo);
            vo.setGranted(grantedIds.contains(permission.getId()));
            vo.setModuleName(resolveModuleName(permission.getModule()));
            result.add(vo);
        }
        result.sort(Comparator
                .comparing(PermissionVO::getModule, Comparator.nullsLast(String::compareTo))
                .thenComparing(PermissionVO::getCode, Comparator.nullsLast(String::compareTo)));
        return result;
    }

    @Override
    public synchronized String add(Map<String, String> params) {
        Assert.notNull(params, "参数不能为空");
        String currentUserName = params.get(UserConfigService.CURRENT_USER_NAME);
        permissionChecker.requirePermission(currentUserName, PERM_USER_EDIT);

        String code = normalizeRoleCode(params.get("code"));
        Assert.isNull(rbacProfile.getRoleByCode(code), "角色编码已存在");

        String name = requireName(params.get("name"));
        String remark = normalizeRemark(params.get("remark"));
        List<Permission> selected = resolveSelectedPermissions(params.get("permissionCodes"));

        long now = System.currentTimeMillis();
        Role role = new Role();
        role.setCode(code);
        role.setName(name);
        role.setBuiltin(0);
        role.setRemark(remark);
        role.setCreateTime(now);
        role.setUpdateTime(now);
        String roleId = rbacProfile.addRole(role);
        Assert.hasText(roleId, "创建角色失败");
        Role created = rbacProfile.getRoleById(roleId);
        if (created == null) {
            created = rbacProfile.getRoleByCode(code);
        }
        Assert.notNull(created, "创建角色失败");
        roleId = created.getId();

        bindPermissions(roleId, selected, now);
        logService.log(LogType.UserLog.INSERT,
                String.format("[%s]新增角色[%s]，权限%d项", currentUserName, code, selected.size()));
        return roleId;
    }

    @Override
    public synchronized String edit(Map<String, String> params) {
        Assert.notNull(params, "参数不能为空");
        String currentUserName = params.get(UserConfigService.CURRENT_USER_NAME);
        permissionChecker.requirePermission(currentUserName, PERM_USER_EDIT);

        String roleId = params.get("roleId");
        Assert.hasText(roleId, "角色ID不能为空");
        Role role = rbacProfile.getRoleById(roleId);
        Assert.notNull(role, "角色不存在");
        Assert.isTrue(role.getBuiltin() != 1, "内置角色不可编辑");

        String name = requireName(params.get("name"));
        String remark = normalizeRemark(params.get("remark"));
        List<Permission> selected = resolveSelectedPermissions(params.get("permissionCodes"));

        long now = System.currentTimeMillis();
        role.setName(name);
        role.setRemark(remark);
        role.setUpdateTime(now);
        rbacProfile.updateRole(role);
        bindPermissions(roleId, selected, now);

        logService.log(LogType.UserLog.UPDATE,
                String.format("[%s]更新角色[%s]权限，共%d项", currentUserName, role.getCode(), selected.size()));
        return roleId;
    }

    @Override
    public synchronized String remove(Map<String, String> params) {
        Assert.notNull(params, "参数不能为空");
        String currentUserName = params.get(UserConfigService.CURRENT_USER_NAME);
        permissionChecker.requirePermission(currentUserName, PERM_USER_EDIT);

        String roleId = params.get("roleId");
        Assert.hasText(roleId, "角色ID不能为空");
        Role role = rbacProfile.getRoleById(roleId);
        Assert.notNull(role, "角色不存在");
        Assert.isTrue(role.getBuiltin() != 1, "内置角色不可删除");

        List<?> bound = rbacProfile.listUserRolesByRoleId(roleId);
        Assert.isTrue(CollectionUtils.isEmpty(bound), "角色仍有用户绑定，无法删除");

        rbacProfile.removeRolePermissionsByRoleId(roleId);
        rbacProfile.removeRole(roleId);
        logService.log(LogType.UserLog.DELETE,
                String.format("[%s]删除角色[%s]", currentUserName, role.getCode()));
        return "删除角色成功";
    }

    private void bindPermissions(String roleId, List<Permission> selected, long now) {
        rbacProfile.removeRolePermissionsByRoleId(roleId);
        if (CollectionUtils.isEmpty(selected)) {
            return;
        }
        for (Permission permission : selected) {
            RolePermission rel = new RolePermission();
            rel.setRoleId(roleId);
            rel.setPermissionId(permission.getId());
            rel.setCreateTime(now);
            rel.setUpdateTime(now);
            rbacProfile.addRolePermission(rel);
        }
    }

    private List<Permission> resolveSelectedPermissions(String permissionCodes) {
        Set<String> codes = parsePermissionCodes(permissionCodes);
        List<Permission> allPermissions = rbacProfile.listPermissions();
        Map<String, Permission> byCode = CollectionUtils.isEmpty(allPermissions)
                ? Collections.emptyMap()
                : allPermissions.stream()
                .filter(p -> p != null && StringUtil.isNotBlank(p.getCode()))
                .collect(Collectors.toMap(Permission::getCode, p -> p, (a, b) -> a));

        List<Permission> selected = new ArrayList<>();
        for (String code : codes) {
            Permission permission = byCode.get(code);
            if (permission == null) {
                throw new BizException("未知权限码: " + code);
            }
            selected.add(permission);
        }
        return selected;
    }

    private String normalizeRoleCode(String raw) {
        Assert.hasText(raw, "角色编码不能为空");
        String code = raw.trim().toLowerCase();
        Assert.isTrue(code.matches("^[a-z][a-z0-9_]{1,31}$"), "角色编码须为小写字母开头，仅含字母数字下划线，长度2~32");
        return code;
    }

    private String requireName(String raw) {
        Assert.hasText(raw, "角色名称不能为空");
        String name = raw.trim();
        Assert.isTrue(name.length() <= 64, "角色名称过长");
        return name;
    }

    private String normalizeRemark(String raw) {
        String remark = raw == null ? "" : raw.trim();
        Assert.isTrue(remark.length() <= 200, "备注过长");
        return remark;
    }

    private RoleVO toRoleVO(Role role, boolean withCodes) {
        RoleVO vo = new RoleVO();
        BeanUtils.copyProperties(role, vo);
        List<RolePermission> rels = rbacProfile.listRolePermissionsByRoleId(role.getId());
        if (CollectionUtils.isEmpty(rels)) {
            vo.setPermissionCount(0);
            vo.setPermissionCodes(Collections.emptyList());
            return vo;
        }
        vo.setPermissionCount(rels.size());
        if (!withCodes) {
            return vo;
        }
        List<String> codes = new ArrayList<>();
        for (RolePermission rel : rels) {
            if (rel == null || StringUtil.isBlank(rel.getPermissionId())) {
                continue;
            }
            Permission permission = rbacProfile.getPermissionById(rel.getPermissionId());
            if (permission != null && StringUtil.isNotBlank(permission.getCode())) {
                codes.add(permission.getCode());
            }
        }
        codes.sort(String::compareTo);
        vo.setPermissionCodes(codes);
        return vo;
    }

    private Set<String> parsePermissionCodes(String raw) {
        if (StringUtil.isBlank(raw)) {
            return Collections.emptySet();
        }
        return Arrays.stream(raw.split(StringUtil.COMMA))
                .map(String::trim)
                .filter(StringUtil::isNotBlank)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private String resolveModuleName(String module) {
        if (StringUtil.isBlank(module)) {
            return "其他";
        }
        String name = MODULE_NAMES.get(module);
        return StringUtil.isNotBlank(name) ? name : module;
    }
}
