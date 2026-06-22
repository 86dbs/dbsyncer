CREATE TABLE `dbsyncer_task_validata_sync_detail` (
   `ID` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '唯一ID',
   `TASK_ID` varchar(64) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '关联的任务id',
   `TYPE` varchar(32) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '任务类型, dataVerification',
   `SOURCE_TABLE_NAME` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT '' COMMENT '数据源表名称',
   `TARGET_TABLE_NAME` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT '' COMMENT '目标源表名称',
   `SOURCE_TOTAL` bigint DEFAULT NULL COMMENT '源表总数',
   `TARGET_TOTAL` bigint DEFAULT NULL COMMENT '目标表总数',
   `DIFF_TOTAL` bigint DEFAULT NULL COMMENT '差异总数',
   `FIXED_TOTAL` bigint DEFAULT NULL COMMENT '订正数量',
   `CONTENT` text CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '对比结果 最高保存100条数据',
   `CREATE_TIME` bigint NOT NULL COMMENT '创建时间',
   `UPDATE_TIME` bigint NOT NULL COMMENT '修改时间',
   PRIMARY KEY (`ID`) USING BTREE,
   KEY `IDX_UPDATE_CREATE_TIME` (`UPDATE_TIME`,`TYPE`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COLLATE = utf8_bin
  COMMENT = '数据校验任务明细表'
  ROW_FORMAT = Dynamic;