CREATE TABLE `dbsyncer_permission` (
  `ID` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '唯一ID',
  `CREATE_TIME` bigint NOT NULL COMMENT '创建时间',
  `UPDATE_TIME` bigint NOT NULL COMMENT '修改时间',
  `CODE` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '权限编码，如 mapping:edit',
  `NAME` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '权限名称',
  `MODULE` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '模块：user/connector/mapping/notice/license/system 等',
  PRIMARY KEY (`ID`) USING BTREE,
  UNIQUE KEY `UK_PERM_CODE` (`CODE`) USING BTREE,
  KEY `IDX_PERM_MODULE` (`MODULE`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = '菜单权限表' ROW_FORMAT = Dynamic;
