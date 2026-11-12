CREATE TABLE `dbsyncer_agent`
(
    `ID`                  varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin  NOT NULL COMMENT '唯一ID',
    `CREATE_TIME`         bigint                                                 NOT NULL COMMENT '创建时间',
    `UPDATE_TIME`         bigint                                                 NOT NULL COMMENT '修改时间',
    `NAME`                varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin  NOT NULL COMMENT 'Agent配置名称',
    `LISTEN_PORT`         int                                                    NOT NULL COMMENT 'Netty服务监听端口',
    `AGENT_SECRET`        varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT 'AGENT密钥，密文',
    `THREAD_NUM`          int                                                    NOT NULL DEFAULT 4 COMMENT '采集/解析线程数',
    `CAPTURE_INTERVAL_MS` int                                                    NOT NULL DEFAULT 1000 COMMENT '采集频率，毫秒',
    `CHECKPOINT`          varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin          DEFAULT 'tail.ckpt' COMMENT '位点文件名',
    `PUBLIC_KEY`          text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '公钥',
    `PRIVATE_KEY`         text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '私钥，密文',
    `JSON`                mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '源库连接与扩展配置',
    `STATE`               tinyint                                                NOT NULL DEFAULT 0 COMMENT '状态，0-停止 1-运行',
    PRIMARY KEY (`ID`) USING BTREE,
    KEY                   `IDX_UPDATE_TIME` (`UPDATE_TIME`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = 'CDC Agent配置表' ROW_FORMAT = Dynamic;
