CREATE TABLE `dbsyncer_config` (
  `ID` varchar(64) COLLATE utf8_bin NOT NULL COMMENT '唯一ID',
  `NAME` varchar(50) COLLATE utf8_bin NOT NULL COMMENT '名称',
  `TYPE` varchar(24) COLLATE utf8_bin NOT NULL COMMENT 'connector、mapping、tableGroup、meta、config',
  `CREATE_TIME` bigint(13) NOT NULL COMMENT '创建时间',
  `UPDATE_TIME` bigint(13) NOT NULL COMMENT '修改时间',
  `JSON` text COLLATE utf8_bin NOT NULL COMMENT '配置信息',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='配置信息表';