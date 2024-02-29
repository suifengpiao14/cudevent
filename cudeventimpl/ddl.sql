
CREATE TABLE `oa`.`department` (
  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `user_id` bigint(11) unsigned NOT NULL  COMMENT '用户ID',
  `user_name` varchar(128) NOT NULL DEFAULT '' COMMENT '用户名称',
  `user_nickname` varchar(128) NOT NULL DEFAULT '' COMMENT '用户昵称',
  `creator_id` varchar(64) NOT NULL DEFAULT '' COMMENT '创建者ID',
  `creator_name` varchar(64) NOT NULL DEFAULT '' COMMENT '创建者名称',
  `deleted_at` datetime DEFAULT NULL COMMENT '删除时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='部门表';

CREATE TABLE `oa`.`user` (
  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `name` varchar(128) NOT NULL DEFAULT '' COMMENT '用户名称',
  `nickname` varchar(128) NOT NULL DEFAULT '' COMMENT '用户昵称',
  `creator_id` varchar(64) NOT NULL DEFAULT '' COMMENT '创建者ID',
  `creator_name` varchar(64) NOT NULL DEFAULT '' COMMENT '创建者名称',
  `deleted_at` datetime DEFAULT NULL COMMENT '删除时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='用户表';


CREATE TABLE `export`.`export_template` (
  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `tenant_id` varchar(128) NOT NULL DEFAULT '' COMMENT '租户标识',
  `template_name` varchar(64) NOT NULL DEFAULT '' COMMENT '模板名称',
  `title` varchar(64) NOT NULL DEFAULT '' COMMENT '导出任务标题',
  `filed_meta` varchar(512) NOT NULL DEFAULT '' COMMENT '导出文件标题和数据字段映射关系[{"name":"data_key","title":"数据标题"}]',
  `http_tpl` text NOT NULL DEFAULT '' COMMENT '代理发起http请求模板',
  `http_script` text NOT NULL DEFAULT '' COMMENT '请求前后执行的脚本',
  `callback_tpl` text NOT NULL DEFAULT '' COMMENT '导出结束后回调请求模板',
  `callback_script` text NOT NULL DEFAULT '' COMMENT '回调前后执行的脚本',
  `request_interval` varchar(10) NOT NULL DEFAULT '1s' COMMENT '循环请求获取数据的间隔时间,单位毫秒-ms,秒-s,小时h',
  `max_exec_time` varchar(15) NOT NULL DEFAULT '' COMMENT '任务处理最长时间,单位秒-s',
  `expired` varchar(10) NOT NULL DEFAULT '1d'  COMMENT '文件过期时间,单位小时-h,月-m',
  `async` enum('true','false') NOT NULL DEFAULT 'true' COMMENT '是否异步执行(true-是,false-否)',
  `remark` varchar(256) NOT NULL DEFAULT '' COMMENT '备注',
  `creator_id` varchar(64) NOT NULL DEFAULT '' COMMENT '创建者ID',
  `creator_name` varchar(64) NOT NULL DEFAULT '' COMMENT '创建者名称',
  `deleted_at` datetime DEFAULT NULL COMMENT '删除时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_name` (`tenant_id`,`template_name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='导出模板表';


CREATE TABLE `export`.`export_task` (
  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `template_id` bigint(11) unsigned NOT NULL default 0 COMMENT 'export_template 表ID',
  `tenant_id` varchar(128) NOT NULL DEFAULT '' COMMENT '租户标识',
  `template_name` varchar(64) NOT NULL DEFAULT '' COMMENT '模板名称',
  `creator_id` varchar(64) NOT NULL DEFAULT '' COMMENT '创建者ID',
  `creator_name` varchar(64) NOT NULL DEFAULT '' COMMENT '创建者名称',
  `filename` varchar(256) NOT NULL DEFAULT '' COMMENT '文件名',
  `title` varchar(64) NOT NULL DEFAULT '' COMMENT '任务标题',
  `md5` varchar(64) NOT NULL DEFAULT '' COMMENT '指纹',
  `status` enum('queuing','exporting','success','fail') NOT NULL DEFAULT 'queuing' COMMENT '任务状态(queuing-排队中,exporting-正在导出,success-成功,fail-失败)',
  `callback_status` enum('init','doing','success','fail') NOT NULL DEFAULT 'init' COMMENT '回调状态(init-初始化,doing-回调中,success-成功,fail-失败)',
  `timeout` varchar(15) NOT NULL DEFAULT '' COMMENT '任务处理超时时间',
  `size` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '文件大小,单位B',
  `url` varchar(256) NOT NULL DEFAULT '' COMMENT '下载地址',
  `remark` varchar(256) NOT NULL DEFAULT '' COMMENT '备注',
  `expired_at` datetime DEFAULT NULL COMMENT '文件过期时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_md5` (`tenant_id`,`md5`),
  KEY `idx_expired_at` (`expired_at`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='下载任务表';
