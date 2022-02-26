/*
 Navicat Premium Data Transfer

 Source Server         : localhost
 Source Server Type    : MySQL
 Source Server Version : 50723
 Source Host           : localhost:3306
 Source Schema         : stock

 Target Server Type    : MySQL
 Target Server Version : 50723
 File Encoding         : 65001

 Date: 26/02/2022 13:46:08
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for daily_report
-- ----------------------------
DROP TABLE IF EXISTS `daily_report`;
CREATE TABLE `daily_report` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增编码',
  `code` varchar(6) NOT NULL DEFAULT '' COMMENT '股票代码',
  `trade_date` varchar(8) NOT NULL DEFAULT '' COMMENT '交易日期',
  `open` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '开盘价',
  `high` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '最高价',
  `low` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '最低价',
  `close` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '收盘价',
  `pre_close` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '昨收价',
  `change` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '涨跌额',
  `pct_chg` decimal(8,2) NOT NULL DEFAULT '0.00' COMMENT '涨跌幅(%)',
  `volume` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '成交量(股)',
  `amount` decimal(16,2) NOT NULL DEFAULT '0.00' COMMENT '成交额(元)',
  PRIMARY KEY (`id`),
  UNIQUE KEY `code_2` (`code`,`trade_date`) USING BTREE,
  KEY `code` (`code`) USING BTREE,
  KEY `trade_date` (`trade_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
