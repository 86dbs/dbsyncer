ALTER TABLE `dbsyncer_upgrade` DROP COLUMN `JSON`;
ALTER TABLE `dbsyncer_upgrade` ADD COLUMN `DATA` blob NOT NULL COMMENT '同步数据';