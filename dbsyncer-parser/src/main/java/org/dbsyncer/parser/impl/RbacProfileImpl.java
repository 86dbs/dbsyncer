/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.parser.impl;

import org.dbsyncer.common.model.Paging;
import org.dbsyncer.common.util.CollectionUtils;
import org.dbsyncer.common.util.StringUtil;
import org.dbsyncer.parser.RbacProfile;
import org.dbsyncer.parser.enums.CommandEnum;
import org.dbsyncer.parser.model.Permission;
import org.dbsyncer.parser.model.Role;
import org.dbsyncer.parser.model.RolePermission;
import org.dbsyncer.parser.model.UserRole;
import org.dbsyncer.parser.util.ConfigModelUtil;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.dbsyncer.sdk.enums.StorageEnum;
import org.dbsyncer.sdk.filter.Query;
import org.dbsyncer.sdk.storage.StorageService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@link RbacProfile} 实现。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
@Component
public class RbacProfileImpl implements RbacProfile {

    @Resource
    private OperationTemplate operationTemplate;

    @Resource
    private StorageService storageService;

    @Override
    public Role getRoleByCode(String code) {
        if (StringUtil.isBlank(code)) {
            return null;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.ROLE);
        query.addFilter(ConfigConstant.ROLE_CODE, code);
        return first(query, Role.class);
    }

    @Override
    public List<Role> listRoles() {
        return operationTemplate.queryList(StorageEnum.ROLE, new Query(), Role.class);
    }

    @Override
    public String addRole(Role role) {
        return operationTemplate.execute(role, CommandEnum.OPR_ADD);
    }

    @Override
    public String updateRole(Role role) {
        return operationTemplate.execute(role, CommandEnum.OPR_EDIT);
    }

    @Override
    public void removeRole(String roleId) {
        if (StringUtil.isBlank(roleId)) {
            return;
        }
        storageService.remove(StorageEnum.ROLE, roleId);
    }

    @Override
    public Permission getPermissionByCode(String code) {
        if (StringUtil.isBlank(code)) {
            return null;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.PERMISSION);
        query.addFilter(ConfigConstant.PERMISSION_CODE, code);
        return first(query, Permission.class);
    }

    @Override
    public List<Permission> listPermissions() {
        return operationTemplate.queryList(StorageEnum.PERMISSION, new Query(), Permission.class);
    }

    @Override
    public void addPermission(Permission permission) {
        operationTemplate.execute(permission, CommandEnum.OPR_ADD);
    }

    @Override
    public RolePermission getRolePermission(String roleId, String permissionId) {
        if (StringUtil.isBlank(roleId) || StringUtil.isBlank(permissionId)) {
            return null;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.ROLE_PERMISSION);
        query.addFilter(ConfigConstant.ROLE_PERMISSION_ROLE_ID, roleId);
        query.addFilter(ConfigConstant.ROLE_PERMISSION_PERMISSION_ID, permissionId);
        return first(query, RolePermission.class);
    }

    @Override
    public List<RolePermission> listRolePermissionsByRoleId(String roleId) {
        if (StringUtil.isBlank(roleId)) {
            return Collections.emptyList();
        }
        Query condition = new Query();
        condition.addFilter(ConfigConstant.ROLE_PERMISSION_ROLE_ID, roleId);
        return operationTemplate.queryList(StorageEnum.ROLE_PERMISSION, condition, RolePermission.class);
    }

    @Override
    public void addRolePermission(RolePermission rel) {
        operationTemplate.execute(rel, CommandEnum.OPR_ADD);
    }

    @Override
    public void removeRolePermissionsByRoleId(String roleId) {
        if (StringUtil.isBlank(roleId)) {
            return;
        }
        Query query = new Query();
        query.setType(StorageEnum.ROLE_PERMISSION);
        query.addFilter(ConfigConstant.ROLE_PERMISSION_ROLE_ID, roleId);
        storageService.delete(query);
    }

    @Override
    public UserRole getUserRole(String userId, String roleId) {
        if (StringUtil.isBlank(userId) || StringUtil.isBlank(roleId)) {
            return null;
        }
        Query query = new Query(1, 1);
        query.setType(StorageEnum.USER_ROLE);
        query.addFilter(ConfigConstant.USER_ROLE_USER_ID, userId);
        query.addFilter(ConfigConstant.USER_ROLE_ROLE_ID, roleId);
        return first(query, UserRole.class);
    }

    @Override
    public List<UserRole> listUserRolesByUserId(String userId) {
        if (StringUtil.isBlank(userId)) {
            return Collections.emptyList();
        }
        Query condition = new Query();
        condition.addFilter(ConfigConstant.USER_ROLE_USER_ID, userId);
        return operationTemplate.queryList(StorageEnum.USER_ROLE, condition, UserRole.class);
    }

    @Override
    public List<UserRole> listUserRolesByRoleId(String roleId) {
        if (StringUtil.isBlank(roleId)) {
            return Collections.emptyList();
        }
        Query condition = new Query();
        condition.addFilter(ConfigConstant.USER_ROLE_ROLE_ID, roleId);
        return operationTemplate.queryList(StorageEnum.USER_ROLE, condition, UserRole.class);
    }

    @Override
    public String addUserRole(UserRole rel) {
        return operationTemplate.execute(rel, CommandEnum.OPR_ADD);
    }

    @Override
    public void removeUserRolesByUserId(String userId) {
        if (StringUtil.isBlank(userId)) {
            return;
        }
        Query query = new Query();
        query.setType(StorageEnum.USER_ROLE);
        query.addFilter(ConfigConstant.USER_ROLE_USER_ID, userId);
        storageService.delete(query);
    }

    @Override
    public Role getRoleById(String id) {
        return operationTemplate.queryObject(Role.class, id);
    }

    @Override
    public Permission getPermissionById(String id) {
        return operationTemplate.queryObject(Permission.class, id);
    }

    private <T> T first(Query query, Class<T> clazz) {
        Paging paging = storageService.query(query);
        if (paging == null || CollectionUtils.isEmpty(paging.getData())) {
            return null;
        }
        Object first = paging.getData().iterator().next();
        if (!(first instanceof Map)) {
            return null;
        }
        T model = ConfigModelUtil.parseFromRow((Map) first, clazz);
        fillType(model);
        return model;
    }

    private void fillType(Object model) {
        if (model instanceof Role) {
            ((Role) model).setType(ConfigConstant.ROLE);
        } else if (model instanceof Permission) {
            ((Permission) model).setType(ConfigConstant.PERMISSION);
        } else if (model instanceof RolePermission) {
            ((RolePermission) model).setType(ConfigConstant.ROLE_PERMISSION);
        } else if (model instanceof UserRole) {
            ((UserRole) model).setType(ConfigConstant.USER_ROLE_REL);
        }
    }
}
