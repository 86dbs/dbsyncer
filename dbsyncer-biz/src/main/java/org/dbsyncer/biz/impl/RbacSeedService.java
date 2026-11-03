/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.biz.impl;

import org.dbsyncer.biz.enums.PermissionModuleEnum;
import org.dbsyncer.parser.RbacProfile;
import org.dbsyncer.parser.model.Permission;
import org.dbsyncer.sdk.constant.ConfigConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;

/**
 * RBAC 种子：幂等灌入菜单权限点。超管按 username=admin 识别，不预置角色。
 *
 * @author wuji
 * @version 1.0.0
 * @date 2026-10-21
 */
@Component
public class RbacSeedService implements ApplicationListener<ContextRefreshedEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private RbacProfile rbacProfile;

    private volatile boolean seeded;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (event.getApplicationContext().getParent() != null || seeded) {
            return;
        }
        seeded = true;
        try {
            seed();
        } catch (Exception e) {
            logger.error("RBAC 种子初始化失败", e);
        }
    }

    private void seed() {
        List<PermissionDef> defs = permissionDefs();
        for (PermissionDef def : defs) {
            ensurePermission(def);
        }
        logger.info("RBAC 种子完成：permissions={}", defs.size());
    }

    private Permission ensurePermission(PermissionDef def) {
        Permission existing = rbacProfile.getPermissionByCode(def.code);
        if (existing != null) {
            return existing;
        }
        long now = System.currentTimeMillis();
        Permission permission = new Permission();
        permission.setType(ConfigConstant.PERMISSION);
        permission.setCode(def.code);
        permission.setName(def.name);
        permission.setModule(def.module);
        permission.setCreateTime(now);
        permission.setUpdateTime(now);
        rbacProfile.addPermission(permission);
        return rbacProfile.getPermissionByCode(def.code);
    }

    private static List<PermissionDef> permissionDefs() {
        return Arrays.asList(
                new PermissionDef("user:view", "查看用户", PermissionModuleEnum.USER),
                new PermissionDef("user:edit", "编辑用户", PermissionModuleEnum.USER),
                new PermissionDef("connector:view", "查看连接器", PermissionModuleEnum.CONNECTOR),
                new PermissionDef("connector:edit", "编辑连接器", PermissionModuleEnum.CONNECTOR),
                new PermissionDef("mapping:view", "查看同步任务", PermissionModuleEnum.MAPPING),
                new PermissionDef("mapping:edit", "编辑同步任务", PermissionModuleEnum.MAPPING),
                new PermissionDef("mapping:operate", "启停同步任务", PermissionModuleEnum.MAPPING),
                new PermissionDef("notice:view", "查看告警配置", PermissionModuleEnum.NOTICE),
                new PermissionDef("notice:edit", "编辑告警配置", PermissionModuleEnum.NOTICE),
                new PermissionDef("license:view", "查看授权", PermissionModuleEnum.LICENSE),
                new PermissionDef("license:edit", "管理授权", PermissionModuleEnum.LICENSE),
                new PermissionDef("system:view", "查看系统配置", PermissionModuleEnum.SYSTEM),
                new PermissionDef("system:edit", "编辑系统配置", PermissionModuleEnum.SYSTEM),
                new PermissionDef("acl:view", "查看资源授权", PermissionModuleEnum.ACL),
                new PermissionDef("acl:edit", "编辑资源授权", PermissionModuleEnum.ACL)
        );
    }

    private static final class PermissionDef {
        private final String code;
        private final String name;
        private final String module;

        private PermissionDef(String code, String name, PermissionModuleEnum module) {
            this.code = code;
            this.name = name;
            this.module = module.getCode();
        }
    }
}
