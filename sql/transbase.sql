-- MySQL dump 10.13  Distrib 5.1.60, for unknown-linux-gnu (x86_64)
--
-- Host: cq01-yyy-hudson0.cq01.xxx.com    Database: transbase
-- ------------------------------------------------------
-- Server version	5.1.60-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `data_info`
--

DROP TABLE IF EXISTS `data_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `data_info` (
  `DID` int(11) NOT NULL AUTO_INCREMENT,
  `dlevel` tinyint(4) DEFAULT NULL,
  `level_num` int(11) DEFAULT NULL,
  `layer_num` tinyint(4) DEFAULT NULL,
  `single_level_num` tinyint(4) DEFAULT NULL,
  `data_path` varchar(200) DEFAULT NULL,
  `module_type` enum('bs','di') DEFAULT NULL,
  `dcomment` char(200) DEFAULT NULL,
  `node_path` char(200) DEFAULT NULL,
  `transfer` enum('0','1') DEFAULT NULL,
  `running` enum('0','1') DEFAULT NULL,
  `finished` enum('0','1') DEFAULT NULL,
  `err` enum('0','1') DEFAULT NULL,
  `cluster` enum('c1','c2','c3') DEFAULT NULL,
  `speedlimit` int(11) DEFAULT '1000',
  `online` enum('0','1') DEFAULT '0',
  PRIMARY KEY (`DID`)
) ENGINE=MyISAM AUTO_INCREMENT=9 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `data_info`
--

LOCK TABLES `data_info` WRITE;
/*!40000 ALTER TABLE `data_info` DISABLE KEYS */;
/*INSERT INTO `data_info` VALUES (...)*/
/*!40000 ALTER TABLE `data_info` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `global_config`
--

DROP TABLE IF EXISTS `global_config`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `global_config` (
  `GID` int(11) NOT NULL AUTO_INCREMENT,
  `trans_cmd` varchar(500) DEFAULT NULL,
  `hdp_basedir` varchar(500) DEFAULT NULL,
  `order` varchar(100) DEFAULT NULL,
  `initialized` enum('0','1') NOT NULL DEFAULT '0',
  `statusstamp` varchar(50) DEFAULT '',
  PRIMARY KEY (`GID`)
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `global_config`
--

LOCK TABLES `global_config` WRITE;
/*!40000 ALTER TABLE `global_config` DISABLE KEYS */;
/*INSERT INTO `global_config` VALUES (1,NULL,'/user/image/build-img/result.bak/result.0816','se vip','0','20120920_222125');*/
/*!40000 ALTER TABLE `global_config` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `node_info`
--

DROP TABLE IF EXISTS `node_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `node_info` (
  `NID` int(11) NOT NULL AUTO_INCREMENT,
  `npath` char(200) NOT NULL,
  `dlevel` tinyint(4) NOT NULL,
  `name` char(20) DEFAULT NULL,
  `level_num` int(11) DEFAULT NULL,
  PRIMARY KEY (`NID`)
) ENGINE=MyISAM AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `node_info`
--

LOCK TABLES `node_info` WRITE;
/*!40000 ALTER TABLE `node_info` DISABLE KEYS */;
/*INSERT INTO `node_info` VALUES (1,'xxx/.../image/frontlevel/search/big',1,'vip',30),(2,'xxx/.../image/frontlevel/search/rare',2,'se',180);*/
/*!40000 ALTER TABLE `node_info` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tasklist`
--

DROP TABLE IF EXISTS `tasklist`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tasklist` (
  `TID` bigint(20) NOT NULL AUTO_INCREMENT,
  `src_path` varchar(200) DEFAULT NULL,
  `dst_server` varchar(200) DEFAULT NULL,
  `data_total` bigint(20) DEFAULT NULL,
  `data_uploaded` bigint(20) DEFAULT NULL,
  `dst_path` varchar(200) DEFAULT NULL,
  `module_type` enum('bs','di') DEFAULT NULL,
  `dlevel` tinyint(4) NOT NULL,
  `cluster` enum('c1','c2','c3','vm') DEFAULT NULL,
  `retry` smallint(4) DEFAULT '0',
  `finished` enum('0','1') DEFAULT NULL,
  `failed` enum('0','1') DEFAULT NULL,
  `level` bigint(20) DEFAULT NULL,
  `layer` int(11) DEFAULT NULL,
  `single_level_num` tinyint(4) DEFAULT NULL,
  `speedlimit` int(11) DEFAULT '1000',
  `statusdir` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`TID`)
) ENGINE=MyISAM AUTO_INCREMENT=1201 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tasklist`
--

LOCK TABLES `tasklist` WRITE;
/*!40000 ALTER TABLE `tasklist` DISABLE KEYS */;
INSERT INTO `tasklist` VALUES (1,'.....')
/*!40000 ALTER TABLE `tasklist` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2012-09-25 12:58:53
