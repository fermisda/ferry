-- MySQL dump 10.13  Distrib 5.1.73, for redhat-linux-gnu (x86_64)
--
-- Host: localhost    Database: voms26_des
-- ------------------------------------------------------
-- Server version	5.1.73

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
-- Current Database: `voms26_des`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `voms26_des` /*!40100 DEFAULT CHARACTER SET latin1 */;

USE `voms26_des`;

--
-- Table structure for table `acl2`
--

DROP TABLE IF EXISTS `acl2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `acl2` (
  `acl_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `group_id` bigint(20) NOT NULL,
  `defaultACL` bit(1) NOT NULL,
  `role_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`acl_id`),
  UNIQUE KEY `group_id` (`group_id`,`defaultACL`,`role_id`),
  KEY `FK2D98E8720C9B10` (`role_id`),
  KEY `FK2D98E8FCFA8B04` (`group_id`)
) ENGINE=InnoDB AUTO_INCREMENT=200 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `acl2`
--

LOCK TABLES `acl2` WRITE;
/*!40000 ALTER TABLE `acl2` DISABLE KEYS */;
INSERT INTO `acl2` VALUES (2,2,'\0',NULL),(12,2,'\0',2),(41,2,'\0',11),(71,2,'\0',21),(132,2,'\0',22),(152,2,'\0',32),(182,2,'\0',42),(196,2,'\0',43),(21,11,'\0',NULL),(31,11,'\0',2),(51,11,'\0',11),(61,11,'\0',21),(122,11,'\0',22),(162,11,'\0',32),(172,11,'\0',42),(193,11,'\0',43),(72,12,'\0',NULL),(82,12,'\0',2),(92,12,'\0',11),(102,12,'\0',21),(112,12,'\0',22),(142,12,'\0',32),(192,12,'\0',42),(199,12,'\0',43);
/*!40000 ALTER TABLE `acl2` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `acl2_permissions`
--

DROP TABLE IF EXISTS `acl2_permissions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `acl2_permissions` (
  `acl_id` bigint(20) NOT NULL,
  `permissions` int(11) DEFAULT NULL,
  `admin_id` bigint(20) NOT NULL,
  PRIMARY KEY (`acl_id`,`admin_id`),
  KEY `FK35C6CFADD91CE8A3` (`acl_id`),
  KEY `FK35C6CFADA4AD9904` (`admin_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `acl2_permissions`
--

LOCK TABLES `acl2_permissions` WRITE;
/*!40000 ALTER TABLE `acl2_permissions` DISABLE KEYS */;
INSERT INTO `acl2_permissions` VALUES (2,16383,2),(2,16383,12),(2,5,32),(2,16383,52),(2,16383,121),(2,16383,131),(2,16383,152),(2,16383,161),(2,16383,162),(2,16383,165),(2,16383,166),(2,16383,172),(2,16383,174),(2,16383,177),(12,16383,2),(12,16383,12),(12,5,32),(12,16383,52),(12,16383,121),(12,16383,131),(12,16383,152),(12,16383,161),(12,16383,162),(12,16383,165),(12,16383,166),(12,16383,172),(12,16383,174),(12,16383,177),(21,16383,2),(21,16383,12),(21,5,32),(21,16383,52),(21,16383,101),(21,16383,111),(21,16383,121),(21,16383,131),(21,16383,141),(21,16383,142),(21,16383,152),(21,16383,161),(21,16383,162),(21,16383,163),(21,16383,165),(21,16383,166),(21,16383,170),(21,16383,172),(21,16383,174),(21,16383,177),(31,16383,2),(31,16383,12),(31,5,32),(31,16383,52),(31,16383,101),(31,16383,111),(31,16383,121),(31,16383,131),(31,16383,141),(31,16383,142),(31,16383,152),(31,16383,161),(31,16383,162),(31,16383,163),(31,16383,165),(31,16383,166),(31,16383,170),(31,16383,172),(31,16383,174),(31,16383,177),(41,16383,2),(41,16383,12),(41,5,32),(41,16383,52),(41,16383,121),(41,16383,131),(41,16383,152),(41,16383,161),(41,16383,162),(41,16383,165),(41,16383,166),(41,16383,172),(41,16383,174),(41,16383,177),(51,16383,2),(51,16383,12),(51,5,32),(51,16383,52),(51,16383,101),(51,16383,111),(51,16383,121),(51,16383,131),(51,16383,141),(51,16383,142),(51,16383,152),(51,16383,161),(51,16383,162),(51,16383,163),(51,16383,165),(51,16383,166),(51,16383,170),(51,16383,172),(51,16383,174),(51,16383,177),(61,16383,2),(61,16383,12),(61,5,32),(61,16383,52),(61,16383,101),(61,16383,111),(61,16383,121),(61,16383,131),(61,16383,141),(61,16383,142),(61,16383,152),(61,16383,161),(61,16383,162),(61,16383,163),(61,16383,165),(61,16383,166),(61,16383,170),(61,16383,172),(61,16383,174),(61,16383,177),(71,16383,2),(71,16383,12),(71,5,32),(71,16383,52),(71,16383,121),(71,16383,131),(71,16383,152),(71,16383,161),(71,16383,162),(71,16383,165),(71,16383,166),(71,16383,172),(71,16383,174),(71,16383,177),(72,16383,2),(72,16383,12),(72,5,32),(72,16383,52),(72,16383,101),(72,16383,111),(72,16383,121),(72,16383,131),(72,16383,141),(72,16383,142),(72,16383,152),(72,16383,161),(72,16383,162),(72,16383,163),(72,16383,165),(72,16383,166),(72,16383,170),(72,16383,172),(72,16383,174),(72,16383,177),(82,16383,2),(82,16383,12),(82,5,32),(82,16383,52),(82,16383,101),(82,16383,111),(82,16383,121),(82,16383,131),(82,16383,141),(82,16383,142),(82,16383,152),(82,16383,161),(82,16383,162),(82,16383,163),(82,16383,165),(82,16383,166),(82,16383,170),(82,16383,172),(82,16383,174),(82,16383,177),(92,16383,2),(92,16383,12),(92,5,32),(92,16383,52),(92,16383,101),(92,16383,111),(92,16383,121),(92,16383,131),(92,16383,141),(92,16383,142),(92,16383,152),(92,16383,161),(92,16383,162),(92,16383,163),(92,16383,165),(92,16383,166),(92,16383,170),(92,16383,172),(92,16383,174),(92,16383,177),(102,16383,2),(102,16383,12),(102,5,32),(102,16383,52),(102,16383,101),(102,16383,111),(102,16383,121),(102,16383,131),(102,16383,141),(102,16383,142),(102,16383,152),(102,16383,161),(102,16383,162),(102,16383,163),(102,16383,165),(102,16383,166),(102,16383,170),(102,16383,172),(102,16383,174),(102,16383,177),(112,16383,2),(112,16383,12),(112,5,32),(112,16383,52),(112,16383,101),(112,16383,111),(112,16383,121),(112,16383,131),(112,16383,141),(112,16383,142),(112,16383,152),(112,16383,161),(112,16383,162),(112,16383,163),(112,16383,165),(112,16383,166),(112,16383,170),(112,16383,172),(112,16383,174),(112,16383,177),(122,16383,2),(122,16383,12),(122,5,32),(122,16383,52),(122,16383,101),(122,16383,111),(122,16383,121),(122,16383,131),(122,16383,141),(122,16383,142),(122,16383,152),(122,16383,161),(122,16383,162),(122,16383,163),(122,16383,165),(122,16383,166),(122,16383,170),(122,16383,172),(122,16383,174),(122,16383,177),(132,16383,2),(132,16383,12),(132,5,32),(132,16383,52),(132,16383,121),(132,16383,131),(132,16383,152),(132,16383,161),(132,16383,162),(132,16383,165),(132,16383,166),(132,16383,172),(132,16383,174),(132,16383,177),(142,16383,2),(142,16383,12),(142,5,32),(142,16383,52),(142,16383,101),(142,16383,111),(142,16383,121),(142,16383,131),(142,16383,141),(142,16383,142),(142,16383,152),(142,16383,161),(142,16383,162),(142,16383,163),(142,16383,165),(142,16383,166),(142,16383,170),(142,16383,172),(142,16383,174),(142,16383,177),(152,16383,2),(152,16383,12),(152,5,32),(152,16383,52),(152,16383,121),(152,16383,131),(152,16383,152),(152,16383,161),(152,16383,162),(152,16383,165),(152,16383,166),(152,16383,172),(152,16383,174),(152,16383,177),(162,16383,2),(162,16383,12),(162,5,32),(162,16383,52),(162,16383,101),(162,16383,111),(162,16383,121),(162,16383,131),(162,16383,141),(162,16383,142),(162,16383,152),(162,16383,161),(162,16383,162),(162,16383,163),(162,16383,165),(162,16383,166),(162,16383,170),(162,16383,172),(162,16383,174),(162,16383,177),(172,16383,2),(172,16383,12),(172,5,32),(172,16383,52),(172,16383,101),(172,16383,111),(172,16383,121),(172,16383,131),(172,16383,141),(172,16383,142),(172,16383,152),(172,16383,161),(172,16383,162),(172,16383,163),(172,16383,165),(172,16383,166),(172,16383,170),(172,16383,172),(172,16383,174),(172,16383,177),(182,16383,2),(182,16383,12),(182,5,32),(182,16383,52),(182,16383,121),(182,16383,131),(182,16383,152),(182,16383,161),(182,16383,162),(182,16383,165),(182,16383,166),(182,16383,172),(182,16383,174),(182,16383,177),(192,16383,2),(192,16383,12),(192,5,32),(192,16383,52),(192,16383,101),(192,16383,111),(192,16383,121),(192,16383,131),(192,16383,141),(192,16383,142),(192,16383,152),(192,16383,161),(192,16383,162),(192,16383,163),(192,16383,165),(192,16383,166),(192,16383,170),(192,16383,172),(192,16383,174),(192,16383,177),(193,16383,2),(193,16383,12),(193,5,32),(193,16383,52),(193,16383,101),(193,16383,111),(193,16383,121),(193,16383,131),(193,16383,141),(193,16383,142),(193,16383,152),(193,16383,161),(193,16383,162),(193,16383,163),(193,16383,165),(193,16383,166),(193,16383,170),(193,16383,172),(193,16383,174),(193,16383,177),(196,16383,2),(196,16383,12),(196,5,32),(196,16383,52),(196,16383,121),(196,16383,131),(196,16383,152),(196,16383,161),(196,16383,162),(196,16383,165),(196,16383,166),(196,16383,172),(196,16383,174),(196,16383,177),(199,16383,2),(199,16383,12),(199,5,32),(199,16383,52),(199,16383,101),(199,16383,111),(199,16383,121),(199,16383,131),(199,16383,141),(199,16383,142),(199,16383,152),(199,16383,161),(199,16383,162),(199,16383,163),(199,16383,165),(199,16383,166),(199,16383,170),(199,16383,172),(199,16383,174),(199,16383,177);
/*!40000 ALTER TABLE `acl2_permissions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `admins`
--

DROP TABLE IF EXISTS `admins`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `admins` (
  `adminid` bigint(20) NOT NULL AUTO_INCREMENT,
  `dn` varchar(255) NOT NULL,
  `email_address` varchar(255) DEFAULT NULL,
  `ca` smallint(6) NOT NULL,
  PRIMARY KEY (`adminid`),
  UNIQUE KEY `dn` (`dn`,`ca`),
  KEY `FKAB3A67047C6FEB32` (`ca`)
) ENGINE=InnoDB AUTO_INCREMENT=178 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `admins`
--

LOCK TABLES `admins` WRITE;
/*!40000 ALTER TABLE `admins` DISABLE KEYS */;
INSERT INTO `admins` VALUES (2,'/O=VOMS/O=System/CN=Internal VOMS Process',NULL,1002),(12,'/O=VOMS/O=System/CN=Local Database Administrator',NULL,1002),(22,'/O=VOMS/O=System/CN=Absolutely Anyone',NULL,1002),(32,'/O=VOMS/O=System/CN=Any Authenticated User',NULL,1002),(42,'/O=VOMS/O=System/CN=Unauthenticated Client',NULL,1002),(52,'/des/Role=VO-Admin',NULL,1022),(101,'/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Keith Chadwick/CN=UID:chadwick',NULL,72),(111,'/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Neha Sharma/CN=UID:neha',NULL,72),(121,'/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Steven C. Timm/CN=UID:timm',NULL,72),(131,'/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Tanya Levshina/CN=UID:tlevshin',NULL,72),(141,'/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=Services/CN=voms2.fnal.gov',NULL,1072),(142,'/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=Services/CN=voms1.fnal.gov',NULL,1072),(152,'/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Nicholas Peregonow/CN=UID:njp',NULL,72),(161,'/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Kenneth R. Herner/CN=UID:kherner',NULL,72),(162,'/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Thomas R. Junk/CN=UID:trj',NULL,72),(163,'/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Merina Albert 2953',NULL,1072),(165,'/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=Services/CN=voms2.fnal.gov','dcso@fnal.gov',1276),(166,'/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=merina/CN=673555/CN=Merina Albert',NULL,1141),(170,'/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=Services/CN=voms4.fnal.gov',NULL,1276),(172,'/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=Services/CN=voms1.fnal.gov',NULL,1276),(174,'/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Steven Timm/CN=UID:timm',NULL,1122),(177,'/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Hyun Kim/CN=UID:hyunwoo',NULL,1122);
/*!40000 ALTER TABLE `admins` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `attributes`
--

DROP TABLE IF EXISTS `attributes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `attributes` (
  `a_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `a_name` varchar(255) NOT NULL,
  `a_desc` text,
  `a_uniq` bit(1) DEFAULT b'0',
  PRIMARY KEY (`a_id`),
  UNIQUE KEY `a_name` (`a_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `attributes`
--

LOCK TABLES `attributes` WRITE;
/*!40000 ALTER TABLE `attributes` DISABLE KEYS */;
/*!40000 ALTER TABLE `attributes` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `aup`
--

DROP TABLE IF EXISTS `aup`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `aup` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `reacceptancePeriod` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `aup`
--

LOCK TABLES `aup` WRITE;
/*!40000 ALTER TABLE `aup` DISABLE KEYS */;
INSERT INTO `aup` VALUES (2,'VO-AUP','',365);
/*!40000 ALTER TABLE `aup` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `aup_acc_record`
--

DROP TABLE IF EXISTS `aup_acc_record`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `aup_acc_record` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `aup_version_id` bigint(20) NOT NULL,
  `usr_id` bigint(20) NOT NULL,
  `last_acceptance_date` datetime NOT NULL,
  `valid` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `aup_version_id` (`aup_version_id`,`usr_id`),
  KEY `FKB1979B32EE2D4487` (`usr_id`),
  KEY `FKB1979B32815F1678` (`aup_version_id`)
) ENGINE=InnoDB AUTO_INCREMENT=505 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `aup_acc_record`
--

LOCK TABLES `aup_acc_record` WRITE;
/*!40000 ALTER TABLE `aup_acc_record` DISABLE KEYS */;
INSERT INTO `aup_acc_record` VALUES (21,2,21,'2016-06-08 11:27:17',''),(51,2,51,'2013-01-30 02:45:12',''),(91,2,91,'2017-02-02 09:50:40',''),(111,2,111,'2014-02-14 09:56:34',''),(121,2,121,'2016-05-02 16:41:46',''),(142,2,142,'2013-01-30 00:00:00',''),(152,2,152,'2013-01-30 00:00:00',''),(161,2,161,'2013-01-30 00:00:00',''),(171,2,171,'2017-02-18 08:57:33',''),(182,2,182,'2013-04-10 12:41:10',''),(192,2,192,'2015-12-21 09:41:26',''),(202,2,202,'2014-01-31 11:17:33',''),(211,2,211,'2016-10-06 17:42:17',''),(212,2,212,'2017-04-25 21:10:43',''),(221,2,221,'2016-09-29 09:38:07',''),(231,2,231,'2017-01-26 22:57:34',''),(241,2,241,'2016-02-02 12:05:06',''),(262,2,262,'2017-03-31 09:44:15',''),(272,2,272,'2014-02-12 15:03:14',''),(282,2,282,'2014-02-13 13:09:10',''),(291,2,291,'2015-05-01 10:42:40',''),(292,2,292,'2014-06-24 11:21:32',''),(301,2,301,'2014-08-04 09:10:35',''),(302,2,302,'2016-11-02 14:05:58',''),(312,2,312,'2016-04-01 17:08:04',''),(322,2,322,'2015-04-07 13:10:57',''),(331,2,331,'2017-05-12 18:23:38',''),(341,2,341,'2016-06-02 15:21:49',''),(351,2,351,'2016-06-16 03:52:14',''),(361,2,361,'2016-06-23 15:42:24',''),(371,2,371,'2016-06-23 14:52:02',''),(381,2,381,'2015-06-09 14:52:12',''),(382,2,382,'2016-07-26 09:57:05',''),(392,2,392,'2015-07-24 15:38:51',''),(401,2,401,'2016-07-29 08:48:22',''),(403,2,403,'2015-09-14 10:49:05',''),(406,2,406,'2015-09-18 10:44:51',''),(409,2,409,'2015-09-18 17:09:57',''),(411,2,411,'2016-10-11 13:15:21',''),(412,2,412,'2017-03-16 20:19:44',''),(415,2,415,'2016-12-21 16:07:31',''),(417,2,417,'2016-01-29 13:58:25',''),(423,2,423,'2016-03-15 14:17:47',''),(427,2,427,'2017-03-31 09:42:26',''),(430,2,433,'2016-04-14 15:12:00',''),(436,2,439,'2016-05-13 02:13:12',''),(439,2,442,'2016-05-13 02:27:47',''),(442,2,445,'2016-05-13 02:39:17',''),(445,2,448,'2016-05-13 02:43:22',''),(448,2,451,'2016-05-13 02:49:03',''),(451,2,454,'2016-05-13 02:56:34',''),(454,2,457,'2016-05-13 11:53:56',''),(457,2,460,'2016-05-13 12:34:06',''),(460,2,463,'2016-05-13 12:38:16',''),(463,2,466,'2016-05-13 12:40:47',''),(466,2,469,'2016-05-13 12:44:29',''),(469,2,475,'2016-05-25 13:26:55',''),(472,2,481,'2016-06-07 11:25:17',''),(475,2,487,'2016-06-13 16:41:07',''),(478,2,490,'2016-06-30 16:58:28',''),(481,2,496,'2016-07-06 17:27:19',''),(484,2,499,'2016-07-12 16:44:05',''),(486,2,501,'2016-09-07 11:16:50',''),(488,2,503,'2016-09-21 19:31:14',''),(491,2,509,'2016-11-04 13:33:14',''),(493,2,511,'2016-12-02 10:26:36',''),(495,2,513,'2016-12-06 13:40:30',''),(498,2,519,'2017-01-04 15:17:11',''),(501,2,525,'2017-01-26 13:57:54',''),(504,2,531,'2017-02-08 12:59:54','');
/*!40000 ALTER TABLE `aup_acc_record` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `aup_version`
--

DROP TABLE IF EXISTS `aup_version`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `aup_version` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `aup_id` bigint(20) NOT NULL,
  `version` varchar(255) NOT NULL,
  `url` varchar(255) DEFAULT NULL,
  `text` varchar(255) DEFAULT NULL,
  `creationTime` datetime NOT NULL,
  `lastForcedReacceptanceTime` datetime DEFAULT NULL,
  `active` bit(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `aup_id` (`aup_id`,`version`),
  KEY `fk_aup_version_aup` (`aup_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `aup_version`
--

LOCK TABLES `aup_version` WRITE;
/*!40000 ALTER TABLE `aup_version` DISABLE KEYS */;
INSERT INTO `aup_version` VALUES (2,2,'1.0','file:////etc/voms-admin/des/vo-aup.txt',NULL,'2012-01-24 12:59:20',NULL,'');
/*!40000 ALTER TABLE `aup_version` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ca`
--

DROP TABLE IF EXISTS `ca`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ca` (
  `cid` smallint(6) NOT NULL AUTO_INCREMENT,
  `subject_string` varchar(255) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `creation_time` datetime NOT NULL,
  PRIMARY KEY (`cid`),
  UNIQUE KEY `subject_string` (`subject_string`)
) ENGINE=InnoDB AUTO_INCREMENT=1327 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ca`
--

LOCK TABLES `ca` WRITE;
/*!40000 ALTER TABLE `ca` DISABLE KEYS */;
INSERT INTO `ca` VALUES (2,'/C=UK/O=eScienceCA/OU=Authority/CN=UK e-Science CA 2A',NULL,'2012-01-24 12:59:19'),(12,'/C=JP/O=KEK/OU=CRC/CN=KEK GRID Certificate Authority',NULL,'2012-01-24 12:59:19'),(22,'/DC=org/DC=cilogon/C=US/O=CILogon/CN=CILogon Silver CA 1',NULL,'2012-01-24 12:59:19'),(32,'/C=AR/O=e-Ciencia/OU=UNLP/L=CeSPI/CN=PKIGrid',NULL,'2012-01-24 12:59:19'),(42,'/C=US/O=National Center for Supercomputing Applications/OU=Certificate Authorities/CN=CACL',NULL,'2012-01-24 12:59:19'),(52,'/C=SY/O=HIAST/CN=HIAST GRID CA',NULL,'2012-01-24 12:59:19'),(62,'/C=BM/O=QuoVadis Limited/OU=Issuing Certification Authority/CN=QuoVadis Grid ICA',NULL,'2012-01-24 12:59:19'),(72,'/DC=gov/DC=fnal/O=Fermilab/OU=Certificate Authorities/CN=Kerberized CA HSM',NULL,'2012-01-24 12:59:19'),(82,'/DC=NET/DC=PRAGMA-GRID/CN=PRAGMA-UCSD CA',NULL,'2012-01-24 12:59:19'),(92,'/C=AM/O=ArmeSFo/CN=ArmeSFo CA',NULL,'2012-01-24 12:59:19'),(102,'/C=JP/O=AIST/OU=GRID/CN=Certificate Authority',NULL,'2012-01-24 12:59:19'),(112,'/DC=es/DC=irisgrid/CN=IRISGridCA',NULL,'2012-01-24 12:59:19'),(122,'/DC=HK/DC=HKU/DC=GRID/CN=HKU Grid CA',NULL,'2012-01-24 12:59:19'),(132,'/C=GR/O=HellasGrid/OU=Certification Authorities/CN=HellasGrid CA 2006',NULL,'2012-01-24 12:59:19'),(142,'/C=MA/O=MaGrid/CN=MaGrid CA',NULL,'2012-01-24 12:59:19'),(152,'/C=IE/O=Grid-Ireland/CN=Grid-Ireland Certification Authority',NULL,'2012-01-24 12:59:19'),(162,'/O=Grid/OU=GlobusTest/OU=simpleCA-mgt6-mgmt.ether.alcf.anl.gov/CN=Globus Simple CA',NULL,'2012-01-24 12:59:19'),(172,'/C=DE/O=DFN-Verein/OU=DFN-PKI/CN=DFN-Verein PCA Grid - G01',NULL,'2012-01-24 12:59:19'),(182,'/C=BM/O=QuoVadis Limited/OU=Root Certification Authority/CN=QuoVadis Root Certification Authority',NULL,'2012-01-24 12:59:19'),(192,'/C=JP/O=National Research Grid Initiative/OU=CGRD/CN=NAREGI CA',NULL,'2012-01-24 12:59:19'),(202,'/C=KR/O=KISTI/O=GRID/CN=KISTI Grid Certificate Authority',NULL,'2012-01-24 12:59:19'),(212,'/C=SK/O=SlovakGrid/CN=SlovakGrid CA',NULL,'2012-01-24 12:59:19'),(222,'/C=AU/O=APACGrid/OU=CA/CN=APACGrid/Email=camanager@vpac.org',NULL,'2012-01-24 12:59:19'),(232,'/C=NL/O=NIKHEF/CN=NIKHEF medium-security certification auth',NULL,'2012-01-24 12:59:19'),(242,'/DC=org/DC=ugrid/CN=UGRID CA',NULL,'2012-01-24 12:59:19'),(252,'/C=PK/O=NCP/CN=PK-GRID-CA',NULL,'2012-01-24 12:59:19'),(262,'/DC=MD/DC=MD-Grid/O=RENAM/OU=Certification Authority/CN=MD-Grid CA',NULL,'2012-01-24 12:59:19'),(272,'/DC=IN/DC=GARUDAINDIA/CN=Indian Grid Certification Authority',NULL,'2012-01-24 12:59:19'),(282,'/DC=CN/DC=Grid/CN=Root Certificate Authority at CNIC',NULL,'2012-01-24 12:59:19'),(292,'/C=RU/O=RDIG/CN=Russian Data-Intensive Grid CA',NULL,'2012-01-24 12:59:19'),(302,'/DC=BR/DC=UFF/DC=IC/O=UFF LACGrid CA/CN=UFF Latin American and Caribbean Catch-all Grid CA',NULL,'2012-01-24 12:59:19'),(312,'/C=SE/O=AddTrust AB/OU=AddTrust External TTP Network/CN=AddTrust External CA Root',NULL,'2012-01-24 12:59:19'),(322,'/C=IL/O=IUCC/CN=IUCC/Email=ca@mail.iucc.ac.il',NULL,'2012-01-24 12:59:19'),(332,'/DC=me/DC=ac/DC=MREN/CN=MREN-CA',NULL,'2012-01-24 12:59:19'),(342,'/DC=org/DC=DOEGrids/OU=Certificate Authorities/CN=DOEGrids CA 1',NULL,'2012-01-24 12:59:19'),(352,'/DC=cz/DC=cesnet-ca/O=CESNET CA/CN=CESNET CA 3',NULL,'2012-01-24 12:59:19'),(362,'/DC=EDU/DC=TENNESSEE/DC=NICS/O=National Institute for Computational Sciences/CN=MyProxy',NULL,'2012-01-24 12:59:19'),(372,'/DC=net/DC=ES/O=ESnet/OU=Certificate Authorities/CN=ESnet Root CA 1',NULL,'2012-01-24 12:59:19'),(382,'/DC=DZ/DC=ARN/O=DZ e-Science GRID/CN=DZ e-Science CA',NULL,'2012-01-24 12:59:19'),(392,'/C=JO/O=JUNet/CN=JUNet CA',NULL,'2012-01-24 12:59:19'),(402,'/C=US/O=Pittsburgh Supercomputing Center/CN=PSC MyProxy CA',NULL,'2012-01-24 12:59:19'),(412,'/DC=bg/DC=acad/CN=BG.ACAD CA',NULL,'2012-01-24 12:59:19'),(422,'/C=UK/O=eScienceCA/OU=Authority/CN=UK e-Science CA 2B',NULL,'2012-01-24 12:59:19'),(432,'/DC=net/DC=ES/OU=Certificate Authorities/CN=NERSC Online CA',NULL,'2012-01-24 12:59:19'),(442,'/DC=EDU/DC=UTEXAS/DC=TACC/O=UT-AUSTIN/CN=TACC Root CA',NULL,'2012-01-24 12:59:19'),(452,'/CN=PurdueCA/O=Purdue University/ST=Indiana/C=US',NULL,'2012-01-24 12:59:19'),(462,'/C=MX/O=UNAMgrid/OU=UNAM/CN=CA',NULL,'2012-01-24 12:59:19'),(472,'/C=NL/O=TERENA/CN=TERENA eScience SSL CA',NULL,'2012-01-24 12:59:19'),(482,'/DC=by/DC=grid/O=uiip.bas-net.by/CN=Belarusian Grid Certification Authority',NULL,'2012-01-24 12:59:19'),(492,'/DC=TW/DC=ORG/DC=NCHC/CN=NCHC CA',NULL,'2012-01-24 12:59:20'),(502,'/C=US/O=National Center for Supercomputing Applications/OU=Certificate Authorities/CN=MyProxy',NULL,'2012-01-24 12:59:20'),(512,'/C=FR/O=CNRS/CN=GRID2-FR',NULL,'2012-01-24 12:59:20'),(522,'/C=IR/O=IPM/O=IRAN-GRID/CN=IRAN-GRID CA',NULL,'2012-01-24 12:59:20'),(532,'/C=GB/ST=Greater Manchester/L=Salford/O=Comodo CA Limited/CN=AAA Certificate Services',NULL,'2012-01-24 12:59:20'),(542,'/DC=ch/DC=cern/CN=CERN Trusted Certification Authority',NULL,'2012-01-24 12:59:20'),(552,'/C=CH/O=SWITCH/CN=SWITCHslcs CA',NULL,'2012-01-24 12:59:20'),(562,'/DC=ch/DC=cern/CN=CERN Root CA',NULL,'2012-01-24 12:59:20'),(572,'/C=US/ST=UT/L=Salt Lake City/O=The USERTRUST Network/OU=http://www.usertrust.com/CN=UTN-USERFirst-Client Authentication and Email',NULL,'2012-01-24 12:59:20'),(582,'/C=MK/O=MARGI/CN=MARGI-CA',NULL,'2012-01-24 12:59:20'),(592,'/C=CO/O=Uniandes CA/O=UNIANDES/OU=DTI/CN=Uniandes CA',NULL,'2012-01-24 12:59:20'),(602,'/C=GR/O=HellasGrid/OU=Certification Authorities/CN=HellasGrid Root CA 2006',NULL,'2012-01-24 12:59:20'),(612,'/DC=CN/DC=Grid/DC=SDG/CN=Scientific Data Grid CA',NULL,'2012-01-24 12:59:20'),(622,'/C=CN/O=HEP/CN=gridca-cn/Email=gridca@ihep.ac.cn',NULL,'2012-01-24 12:59:20'),(632,'/CN=Purdue TeraGrid RA/OU=Purdue TeraGrid/O=Purdue University/ST=Indiana/C=US',NULL,'2012-01-24 12:59:20'),(642,'/C=BE/OU=BEGRID/O=BELNET/CN=BEgrid CA',NULL,'2012-01-24 12:59:20'),(652,'/C=RS/O=AEGIS/CN=AEGIS-CA',NULL,'2012-01-24 12:59:20'),(662,'/O=Grid/O=NorduGrid/CN=NorduGrid Certification Authority',NULL,'2012-01-24 12:59:20'),(672,'/DC=cz/DC=cesnet-ca/O=CESNET CA/CN=CESNET CA Root',NULL,'2012-01-24 12:59:20'),(682,'/C=FR/O=CNRS/CN=CNRS2-Projets',NULL,'2012-01-24 12:59:20'),(692,'/DC=org/DC=balticgrid/CN=Baltic Grid Certification Authority',NULL,'2012-01-24 12:59:20'),(702,'/C=TR/O=TRGrid/CN=TR-Grid CA',NULL,'2012-01-24 12:59:20'),(712,'/C=US/ST=UT/L=Salt Lake City/O=The USERTRUST Network/OU=http://www.usertrust.com/CN=UTN-USERFirst-Hardware',NULL,'2012-01-24 12:59:20'),(722,'/DC=RO/DC=RomanianGRID/O=ROSA/OU=Certification Authority/CN=RomanianGRID CA',NULL,'2012-01-24 12:59:20'),(732,'/C=VE/O=Grid/O=Universidad de Los Andes/OU=CeCalCULA/CN=ULAGrid Certification Authority',NULL,'2012-01-24 12:59:20'),(742,'/C=CA/O=Grid/CN=Grid Canada Certificate Authority',NULL,'2012-01-24 12:59:20'),(752,'/C=PL/O=GRID/CN=Polish Grid CA',NULL,'2012-01-24 12:59:20'),(762,'/C=PT/O=LIPCA/CN=LIP Certification Authority',NULL,'2012-01-24 12:59:20'),(772,'/DC=cz/DC=cesnet-ca/CN=CESNET CA',NULL,'2012-01-24 12:59:20'),(782,'/C=IT/O=INFN/CN=INFN CA',NULL,'2012-01-24 12:59:20'),(792,'/DC=EDU/DC=UTEXAS/DC=TACC/O=UT-AUSTIN/CN=TACC MICS CA',NULL,'2012-01-24 12:59:20'),(802,'/DC=ORG/DC=SEE-GRID/CN=SEE-GRID CA',NULL,'2012-01-24 12:59:20'),(812,'/C=NL/O=TERENA/CN=TERENA eScience Personal CA',NULL,'2012-01-24 12:59:20'),(822,'/C=DE/O=DFN-Verein/OU=DFN-PKI/CN=DFN SLCS-CA',NULL,'2012-01-24 12:59:20'),(832,'/C=HU/O=NIIF/OU=Certificate Authorities/CN=NIIF Root CA',NULL,'2012-01-24 12:59:20'),(842,'/C=SI/O=SiGNET/CN=SiGNET CA',NULL,'2012-01-24 12:59:20'),(852,'/C=DE/O=GermanGrid/CN=GridKa-CA',NULL,'2012-01-24 12:59:20'),(862,'/C=UK/O=eScienceCA/OU=Authority/CN=UK e-Science CA',NULL,'2012-01-24 12:59:20'),(872,'/C=CH/O=Switch - Teleinformatikdienste fuer Lehre und Forschung/CN=SWITCHgrid Root CA',NULL,'2012-01-24 12:59:20'),(882,'/C=UK/O=eScienceRoot/OU=Authority/CN=UK e-Science Root',NULL,'2012-01-24 12:59:20'),(892,'/C=HR/O=edu/OU=srce/CN=SRCE CA',NULL,'2012-01-24 12:59:20'),(902,'/C=TH/O=NECTEC/OU=GOC/CN=NECTEC GOC CA',NULL,'2012-01-24 12:59:20'),(912,'/C=US/O=National Center for Supercomputing Applications/OU=Certificate Authorities/CN=GridShib CA',NULL,'2012-01-24 12:59:20'),(922,'/DC=EDU/DC=UTEXAS/DC=TACC/O=UT-AUSTIN/CN=TACC Classic CA',NULL,'2012-01-24 12:59:20'),(932,'/C=TW/O=AS/CN=Academia Sinica Grid Computing Certification Authority Mercury',NULL,'2012-01-24 12:59:20'),(942,'/C=CY/O=CyGrid/O=HPCL/CN=CyGridCA',NULL,'2012-01-24 12:59:20'),(952,'/DC=LV/DC=latgrid/CN=Certification Authority for Latvian Grid',NULL,'2012-01-24 12:59:20'),(962,'/C=BR/O=ICPEDU/O=UFF BrGrid CA/CN=UFF Brazilian Grid Certification Authority',NULL,'2012-01-24 12:59:20'),(972,'/C=AT/O=AustrianGrid/OU=Certification Authority/CN=Certificate Issuer',NULL,'2012-01-24 12:59:20'),(982,'/C=FR/O=CNRS/CN=CNRS2',NULL,'2012-01-24 12:59:20'),(992,'/C=CL/O=REUNACA/CN=REUNA Certification Authority',NULL,'2012-01-24 12:59:20'),(1002,'/O=VOMS/O=System/CN=Dummy Certificate Authority','A dummy CA for local org.glite.security.voms.admin.persistence.error mainteneance','2012-01-24 12:59:20'),(1012,'/O=VOMS/O=System/CN=VOMS Group','A virtual CA for VOMS groups.','2012-01-24 12:59:20'),(1022,'/O=VOMS/O=System/CN=VOMS Role','A virtual CA for VOMS roles.','2012-01-24 12:59:20'),(1032,'/O=VOMS/O=System/CN=Authorization Manager Attributes','A virtual CA for authz manager attributes','2012-01-24 12:59:20'),(1042,'/DC=com/DC=DigiCert-Grid/O=DigiCert Grid/CN=DigiCert Grid Root CA',NULL,'2012-02-15 05:33:03'),(1052,'/C=US/O=DigiCert Grid/OU=www.digicert.com/CN=DigiCert Grid Trust CA',NULL,'2012-02-15 05:33:03'),(1062,'/C=US/O=DigiCert Inc/OU=www.digicert.com/CN=DigiCert Assured ID Root CA',NULL,'2012-02-15 05:33:03'),(1072,'/DC=com/DC=DigiCert-Grid/O=DigiCert Grid/CN=DigiCert Grid CA-1',NULL,'2012-02-15 05:33:03'),(1081,'/C=US/O=National Center for Supercomputing Applications/OU=Certificate Authorities/CN=Two Factor CA',NULL,'2012-05-28 19:46:57'),(1091,'/DC=MY/DC=UPM/DC=MYIFAM/C=MY/O=MYIFAM/CN=Malaysian Identity Federation and Access Management',NULL,'2012-05-28 19:46:58'),(1101,'/C=EG/O=EG-GRID/CN=EG-GRID Certification Authority',NULL,'2012-10-09 13:55:45'),(1111,'/C=BR/O=ANSP/OU=ANSPGrid CA/CN=ANSPGrid CA',NULL,'2012-10-09 13:55:45'),(1112,'/DC=org/DC=cilogon/C=US/O=CILogon/CN=CILogon OpenID CA 1',NULL,'2013-04-07 17:15:40'),(1122,'/DC=org/DC=cilogon/C=US/O=CILogon/CN=CILogon Basic CA 1',NULL,'2013-04-07 17:15:40'),(1131,'/C=CN/O=HEP/CN=Institute of High Energy Physics Certification Authority',NULL,'2013-06-27 15:10:35'),(1141,'/DC=ch/DC=cern/CN=CERN Grid Certification Authority',NULL,'2013-07-27 23:54:53'),(1151,'/C=ch/O=CERN/CN=CERN Root Certification Authority 2',NULL,'2013-07-27 23:54:54'),(1161,'/DC=ORG/DC=SEE-GRID/CN=SEE-GRID CA 2013',NULL,'2013-12-09 21:50:41'),(1171,'/C=US/O=National Center for Supercomputing Applications/OU=Certificate Authorities/CN=Two Factor CA 2013',NULL,'2013-12-09 21:50:42'),(1181,'/C=US/O=National Center for Supercomputing Applications/OU=Certificate Authorities/CN=MyProxy CA 2013',NULL,'2013-12-09 21:50:42'),(1182,'/C=US/O=DigiCert Grid/OU=www.digicert.com/CN=DigiCert Grid Trust CA G2',NULL,'2014-06-04 17:18:46'),(1192,'/DC=DigiCert-Grid/DC=com/O=DigiCert Grid/CN=DigiCert Grid CA-1 G2',NULL,'2014-06-04 17:18:46'),(1202,'/C=GB/ST=Greater Manchester/L=Salford/O=COMODO CA Limited/CN=COMODO RSA Certification Authority',NULL,'2014-07-22 10:13:43'),(1212,'/C=US/O=Internet2/OU=InCommon/CN=InCommon IGTF Server CA',NULL,'2014-07-22 10:13:43'),(1222,'/DC=GE/DC=TSU/CN=TSU Root CA',NULL,'2014-12-17 18:50:22'),(1232,'/C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience SSL CA 2',NULL,'2014-12-17 18:50:22'),(1242,'/C=US/ST=New Jersey/L=Jersey City/O=The USERTRUST Network/CN=USERTrust RSA Certification Authority',NULL,'2014-12-17 18:50:22'),(1252,'/C=JP/O=NII/OU=HPCI/CN=HPCI CA',NULL,'2014-12-17 18:50:22'),(1262,'/C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience Personal CA 2',NULL,'2014-12-17 18:50:22'),(1264,'/C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience Personal CA 3',NULL,'2015-11-02 13:22:59'),(1267,'/C=NL/ST=Noord-Holland/L=Amsterdam/O=TERENA/CN=TERENA eScience SSL CA 3',NULL,'2015-11-02 13:22:59'),(1270,'/DC=com/DC=DigiCert-Grid/O=DigiCert Grid/CN=DigiCert Grid CA-1 G2',NULL,'2015-11-02 13:22:59'),(1273,'/C=HU/O=NIIF/OU=Certificate Authorities/CN=NIIF Root CA 2',NULL,'2015-11-02 13:22:59'),(1276,'/DC=org/DC=cilogon/C=US/O=CILogon/CN=CILogon OSG CA 1',NULL,'2015-11-10 13:48:40'),(1279,'/C=IT/O=INFN/CN=INFN Certification Authority',NULL,'2015-11-10 13:48:40'),(1281,'/O=Grid/O=NorduGrid/CN=NorduGrid Certification Authority 2015',NULL,'2016-01-06 10:03:56'),(1283,'/DC=CN/DC=Grid/DC=SDG/CN=Scientific Data Grid CA - G2',NULL,'2016-03-16 14:47:49'),(1286,'/DC=ke/DC=kenet/O=Kenya Education Network Trust/OU=Research Services/CN=KENET CA',NULL,'2016-03-16 14:47:49'),(1289,'/DC=ke/DC=kenet/O=Kenya Education Network Trust/OU=Research Services/CN=KENET ROOT CA',NULL,'2016-03-16 14:47:49'),(1292,'/DC=ch/DC=cern/CN=CERN LCG IOTA Certification Authority',NULL,'2016-03-16 14:47:49'),(1295,'/C=GR/O=HellasGrid/OU=Certification Authorities/CN=HellasGrid CA 2016',NULL,'2016-06-15 08:51:28'),(1299,'/DC=eu/DC=rcauth/O=Certification Authorities/CN=Research and Collaboration Authentication Pilot G1 CA',NULL,'2016-08-09 22:52:00'),(1302,'/DC=nl/DC=dutchgrid/O=Certification Authorities/CN=DCA Root G1 CA',NULL,'2016-08-09 22:52:00'),(1304,'/C=BM/O=QuoVadis Limited/CN=QuoVadis Root CA 2',NULL,'2016-10-19 22:06:39'),(1307,'/C=BM/O=QuoVadis Limited/CN=QuoVadis Grid ICA G2',NULL,'2016-10-19 22:06:39'),(1311,'/C=IR/O=IPM/OU=GCG/CN=IRAN-GRID-G2 CA',NULL,'2017-02-02 13:57:17'),(1315,'/C=BM/O=QuoVadis Limited/CN=QuoVadis Root CA 2 G3',NULL,'2017-02-27 20:43:29'),(1318,'/C=AE/O=DarkMatter LLC/CN=DarkMatter Assured CA',NULL,'2017-02-27 20:43:30'),(1321,'/C=AE/O=DarkMatter LLC/CN=DarkMatter Secure CA',NULL,'2017-02-27 20:43:30'),(1324,'/C=BM/O=QuoVadis Limited/CN=QuoVadis Root CA 3 G3',NULL,'2017-02-27 20:43:30'),(1326,'/DC=org/DC=ugrid/CN=UGRID CA G2',NULL,'2017-04-11 05:37:31');
/*!40000 ALTER TABLE `ca` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `capabilities`
--

DROP TABLE IF EXISTS `capabilities`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `capabilities` (
  `cid` bigint(20) NOT NULL AUTO_INCREMENT,
  `capability` varchar(255) NOT NULL,
  PRIMARY KEY (`cid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `capabilities`
--

LOCK TABLES `capabilities` WRITE;
/*!40000 ALTER TABLE `capabilities` DISABLE KEYS */;
/*!40000 ALTER TABLE `capabilities` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `certificate`
--

DROP TABLE IF EXISTS `certificate`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `certificate` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `creation_time` datetime NOT NULL,
  `subject_string` varchar(255) NOT NULL,
  `suspended` bit(1) NOT NULL,
  `suspended_reason` varchar(255) DEFAULT NULL,
  `suspension_reason_code` varchar(255) DEFAULT NULL,
  `ca_id` smallint(6) NOT NULL,
  `usr_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ca_id` (`ca_id`,`subject_string`),
  KEY `FK745F4197EE2D4487` (`usr_id`),
  KEY `FK745F419782107F70` (`ca_id`)
) ENGINE=InnoDB AUTO_INCREMENT=131602 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `certificate`
--

LOCK TABLES `certificate` WRITE;
/*!40000 ALTER TABLE `certificate` DISABLE KEYS */;
INSERT INTO `certificate` VALUES (51,'2012-01-30 13:00:16','/DC=es/DC=irisgrid/O=pic/CN=christian.neissner','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',112,51),(111,'2012-01-30 13:00:18','/DC=org/DC=doegrids/OU=People/CN=Gabriele Garzoglio 762243','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',342,111),(121,'2012-01-30 13:00:19','/DC=org/DC=doegrids/OU=People/CN=Marko Slyz 664315','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',342,121),(142,'2012-08-10 17:13:19','/DC=org/DC=doegrids/OU=People/CN=Chad Kerner 543877','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',342,142),(152,'2012-10-02 16:29:36','/C=US/O=National Center for Supercomputing Applications/OU=People/CN=Weddie Jackson','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',42,152),(161,'2012-10-18 16:41:48','/DC=org/DC=doegrids/OU=People/CN=Tanya Levshina 508821','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',342,161),(171,'2012-11-07 13:58:43','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Brian P. Yanny/CN=UID:yanny','\0',NULL,NULL,72,171),(182,'2013-04-10 12:41:09','/C=US/O=National Center for Supercomputing Applications/OU=People/CN=Ankit Chandra','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',42,182),(192,'2013-07-22 09:50:34','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Joe B. Boyd/CN=UID:boyd','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,192),(202,'2013-09-03 16:39:54','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Marko Slyz 700','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1072,202),(211,'2013-09-05 12:11:31','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Marcelle Soares-Santos 1836','\0',NULL,NULL,1072,211),(212,'2013-09-26 16:36:06','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Greg Daues 1912','\0',NULL,NULL,1072,212),(221,'2013-09-27 12:34:05','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Michelle Gower 1913','\0',NULL,NULL,1072,221),(231,'2013-09-27 12:43:44','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Karl Drlica-Wagner 1914','\0',NULL,NULL,1072,231),(241,'2014-01-30 08:44:43','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Steven C. Timm/CN=UID:timm','\0',NULL,NULL,72,91),(252,'2014-01-30 17:05:02','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Gabriele Garzoglio 203','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1072,111),(262,'2014-01-30 17:06:01','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Gabriele Garzoglio/CN=UID:garzogli','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,111),(302,'2014-02-03 10:36:04','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Michael Johnson 2274','\0',NULL,NULL,1072,262),(312,'2014-02-04 08:47:44','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Joe Boyd 579','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1072,192),(322,'2014-02-12 15:03:14','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Bodhitha Jayatilaka 2312','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1072,272),(332,'2014-02-13 13:09:10','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Bodhitha Jayatilaka/CN=UID:boj','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,282),(341,'2014-04-16 10:31:40','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Nicholas Peregonow/CN=UID:njp','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,291),(342,'2014-06-24 11:21:32','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Ricardo Covarrubias 2589','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1072,292),(351,'2014-08-04 09:10:35','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Kathleen Grabowski/CN=UID:kgrabow','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,301),(382,'2015-01-09 14:19:31','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Nikolay P. Kuropatkin/CN=UID:kuropat','\0',NULL,NULL,72,302),(401,'2015-01-09 14:36:36','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Nikolay P. Kuropatkin/CN=UID:kuropat','\0',NULL,NULL,72,302),(421,'2015-01-12 10:27:27','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Nicholas Peregonow/CN=UID:njp','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,291),(441,'2015-01-12 10:27:39','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Steven C. Timm/CN=UID:timm','\0',NULL,NULL,72,91),(461,'2015-01-12 10:27:52','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Gerard Bernabeu Altayo/CN=UID:gerard1','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,241),(471,'2015-01-12 10:27:53','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=gpsn01.fnal.gov/CN=cron/CN=Gerard Bernabeu Altayo/CN=UID:gerard1','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,241),(481,'2015-01-12 10:28:03','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Joe B. Boyd/CN=UID:boyd','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,192),(521,'2015-01-12 10:28:40','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Gabriele Garzoglio/CN=UID:garzogli','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,111),(541,'2015-01-12 10:28:54','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Kathleen Grabowski/CN=UID:kgrabow','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,301),(561,'2015-01-12 10:29:09','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Bodhitha Jayatilaka/CN=UID:boj','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,282),(581,'2015-01-12 10:29:40','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Brian P. Yanny/CN=UID:yanny','\0',NULL,NULL,72,171),(601,'2015-02-24 10:23:31','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Gerard Bernabeu Altayo/CN=UID:gerard1','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,241),(602,'2015-03-04 16:56:09','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Dennis D. Box/CN=UID:dbox','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,312),(611,'2015-03-05 09:44:34','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Dennis Box 497','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1072,312),(612,'2015-03-05 09:54:33','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Dennis D. Box/CN=UID:dbox','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,312),(622,'2015-04-07 13:10:57','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Felipe Menanteau 3137','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1072,322),(631,'2015-05-05 11:38:30','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Kenneth R. Herner/CN=UID:kherner','\0',NULL,NULL,72,331),(632,'2015-05-13 14:40:54','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Kenneth R. Herner/CN=UID:kherner','\0',NULL,NULL,72,331),(641,'2015-05-19 14:09:45','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Masao Sako 3215','\0',NULL,NULL,1072,341),(642,'2015-05-29 14:35:10','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Marcelle Soares-Santos/CN=UID:marcelle','\0',NULL,NULL,72,211),(651,'2015-06-01 11:47:30','/C=UK/O=eScience/OU=Manchester/L=HEP/CN=joe zuntz','\0',NULL,NULL,422,351),(652,'2015-06-03 16:36:24','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Marcelle S. Santos/CN=UID:marcelle','\0',NULL,NULL,72,211),(661,'2015-06-04 12:21:48','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Masao Sako/CN=UID:masao','\0',NULL,NULL,72,341),(671,'2015-06-04 16:04:47','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Masao Sako/CN=UID:masao','\0',NULL,NULL,72,341),(681,'2015-06-05 11:56:19','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Flavia Sobreira sanchez/CN=UID:sobreira','\0',NULL,NULL,72,361),(691,'2015-06-05 12:09:34','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Flavia Sobreira sanchez/CN=UID:sobreira','\0',NULL,NULL,72,361),(701,'2015-06-05 16:00:49','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Marcelle S. Santos/CN=UID:marcelle','\0',NULL,NULL,72,211),(711,'2015-06-09 10:41:47','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Stephen M. Kent/CN=UID:skent','\0',NULL,NULL,72,21),(712,'2015-06-09 11:03:37','CN=fifegrid,CN=batch,CN=Stephen M. Kent,CN=UID:skent','\0',NULL,NULL,72,21),(721,'2015-06-09 14:39:04','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Stephen M. Kent/CN=UID:skent','\0',NULL,NULL,72,21),(731,'2015-06-09 14:45:02','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Huan Lin/CN=UID:hlin','\0',NULL,NULL,72,371),(741,'2015-06-09 14:52:12','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Lalith P. Perera/CN=UID:perera','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,381),(751,'2015-06-09 14:54:19','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Huan Lin/CN=UID:hlin','\0',NULL,NULL,72,371),(761,'2015-06-09 14:55:28','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Lalith P. Perera/CN=UID:perera','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,381),(762,'2015-07-07 10:40:13','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Eric Morganson 3317','\0',NULL,NULL,1072,382),(771,'2015-07-13 09:02:56','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Joseph A. Zuntz/CN=UID:joezuntz','\0',NULL,NULL,72,351),(772,'2015-07-13 09:09:03','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Joseph A. Zuntz/CN=UID:joezuntz','\0',NULL,NULL,72,351),(782,'2015-07-24 15:38:51','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Thomas R. Junk/CN=UID:trj','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,392),(791,'2015-07-29 15:25:59','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Douglas Nathaniel Friedel 3371','\0',NULL,NULL,1072,401),(792,'2015-09-03 19:01:06','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=Services/CN=desgw/des41.fnal.gov','\0',NULL,NULL,1072,331),(793,'2015-09-14 10:49:05','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Dmitry O. Litvintsev/CN=UID:litvinse','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,403),(796,'2015-09-18 10:44:51','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Douglas L. Tucker/CN=UID:dtucker','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,406),(799,'2015-09-18 17:09:57','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Sahar Allam/CN=UID:sallam','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,409),(801,'2015-09-22 15:35:29','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Dmitry Litvintsev 1123','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1072,403),(804,'2015-09-23 15:32:03','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Douglas L. Tucker/CN=UID:dtucker','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,406),(805,'2015-09-23 15:49:19','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Sahar Allam/CN=UID:sallam','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,409),(810,'2015-10-12 10:15:25','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Eric H. NeilsenJr./CN=UID:neilsen','\0',NULL,NULL,72,411),(813,'2015-10-12 10:27:16','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Eric H. NeilsenJr./CN=UID:neilsen','\0',NULL,NULL,72,411),(814,'2015-11-22 12:52:52','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Greg Daues 1912','\0',NULL,NULL,1270,412),(817,'2015-12-16 15:46:47','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=Services/CN=frontend/fifebatch.fnal.gov','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1276,192),(820,'2015-12-22 14:11:16','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=James Annis/CN=UID:annis','\0',NULL,NULL,72,415),(823,'2015-12-22 14:13:20','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=James Annis/CN=UID:annis','\0',NULL,NULL,72,415),(825,'2016-01-22 15:01:29','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Alex Drlica-Wagner/CN=UID:kadrlica','\0',NULL,NULL,72,231),(828,'2016-01-29 13:58:25','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Xinyang Lu 3630','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1270,417),(834,'2016-01-29 14:49:39','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Alex Drlica-wagner/CN=UID:kadrlica','\0',NULL,NULL,72,231),(932,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Bodhitha Jayatilaka 2312','\0',NULL,NULL,1276,272),(935,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Dennis Box 497','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1276,312),(938,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Dmitry Litvintsev 1123','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1276,403),(941,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Douglas Nathaniel Friedel 3371','\0',NULL,NULL,1276,401),(944,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Eric Morganson 3317','\0',NULL,NULL,1276,382),(947,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Felipe Menanteau 3137','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1276,322),(950,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Gabriele Garzoglio 203','\0',NULL,NULL,1276,111),(953,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Greg Daues 1912','\0',NULL,NULL,1276,212),(956,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Joe Boyd 579','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1276,192),(959,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Karl Drlica-Wagner 1914','\0',NULL,NULL,1276,231),(962,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Marcelle Soares-Santos 1836','\0',NULL,NULL,1276,211),(965,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Marko Slyz 700','\0',NULL,NULL,1276,202),(968,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Masao Sako 3215','\0',NULL,NULL,1276,341),(971,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Michael Johnson 2274','\0',NULL,NULL,1276,262),(974,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Michelle Gower 1913','\0',NULL,NULL,1276,221),(980,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Nikolay Kuropatkin 51','\0',NULL,NULL,1276,302),(983,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Ricardo Covarrubias 2589','\0',NULL,NULL,1276,292),(986,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Steve Timm 167','\0',NULL,NULL,1276,91),(989,'2016-03-07 13:46:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=Services/CN=desgw/des41.fnal.gov','\0',NULL,NULL,1276,331),(990,'2016-03-15 14:17:47','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Mandeep Gill/CN=UID:mssgill','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,423),(996,'2016-03-15 14:22:36','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Mandeep Gill/CN=UID:mssgill','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,423),(1000,'2016-03-28 11:01:36','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Michael Johnson 2274','\0',NULL,NULL,1270,427),(1006,'2016-04-06 11:16:47','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=James Annis/CN=UID:annis','\0',NULL,NULL,1122,415),(1009,'2016-04-06 11:16:56','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Bodhitha Jayatilaka/CN=UID:boj','\0',NULL,NULL,1122,282),(1012,'2016-04-06 11:16:57','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Joe Boyd/CN=UID:boyd','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,192),(1015,'2016-04-06 11:17:07','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Dennis Box/CN=UID:dbox','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,312),(1018,'2016-04-06 11:17:13','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Douglas Tucker/CN=UID:dtucker','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,406),(1021,'2016-04-06 11:17:21','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Gabriele Garzoglio/CN=UID:garzogli','\0',NULL,NULL,1122,111),(1024,'2016-04-06 11:17:22','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Gerard Bernabeu Altayo/CN=UID:gerard1','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,241),(1027,'2016-04-06 11:17:29','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Huan Lin/CN=UID:hlin','\0',NULL,NULL,1122,371),(1030,'2016-04-06 11:17:41','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Joseph Zuntz/CN=UID:joezuntz','\0',NULL,NULL,1122,351),(1033,'2016-04-06 11:17:45','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Alex Drlica-wagner/CN=UID:kadrlica','\0',NULL,NULL,1122,231),(1036,'2016-04-06 11:17:48','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Kathleen Grabowski/CN=UID:kgrabow','\0',NULL,NULL,1122,301),(1039,'2016-04-06 11:17:48','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Kenneth Herner/CN=UID:kherner','\0',NULL,NULL,1122,331),(1045,'2016-04-06 11:17:56','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Dmitry Litvintsev/CN=UID:litvinse','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,403),(1048,'2016-04-06 11:18:01','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Marcelle Santos/CN=UID:marcelle','\0',NULL,NULL,1122,211),(1051,'2016-04-06 11:18:02','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Masao Sako/CN=UID:masao','\0',NULL,NULL,1122,341),(1054,'2016-04-06 11:18:12','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Mandeep Gill/CN=UID:mssgill','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,423),(1060,'2016-04-06 11:18:14','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Eric Neilsen/CN=UID:neilsen','\0',NULL,NULL,1122,411),(1063,'2016-04-06 11:18:15','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Nicholas Peregonow/CN=UID:njp','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,291),(1066,'2016-04-06 11:18:21','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Lalith Perera/CN=UID:perera','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,381),(1069,'2016-04-06 11:18:34','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Sahar Allam/CN=UID:sallam','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,409),(1072,'2016-04-06 11:18:40','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Stephen Kent/CN=UID:skent','\0',NULL,NULL,1122,21),(1075,'2016-04-06 11:18:42','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Flavia Sobreira sanchez/CN=UID:sobreira','\0',NULL,NULL,1122,361),(1078,'2016-04-06 11:18:50','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Steven Timm/CN=UID:timm','\0',NULL,NULL,1122,91),(1081,'2016-04-06 11:18:53','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Thomas Junk/CN=UID:trj','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,392),(1084,'2016-04-06 11:19:01','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Brian Yanny/CN=UID:yanny','\0',NULL,NULL,1122,171),(1090,'2016-04-14 15:12:00','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Thomas Diehl/CN=UID:diehl','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,433),(1093,'2016-04-14 15:14:03','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Thomas Diehl/CN=UID:diehl','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,433),(1099,'2016-05-13 02:13:12','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Stephanie J. Hamilton/CN=UID:hamil332','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,439),(1102,'2016-05-13 02:21:26','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Stephanie J. Hamilton/CN=UID:hamil332','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,439),(1105,'2016-05-13 02:22:13','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Stephanie Hamilton/CN=UID:hamil332','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,439),(1108,'2016-05-13 02:27:47','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Ting Li/CN=UID:tingli','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,442),(1111,'2016-05-13 02:28:40','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Ting Li/CN=UID:tingli','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,442),(1114,'2016-05-13 02:29:12','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Ting Li/CN=UID:tingli','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,442),(1117,'2016-05-13 02:34:39','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Kenneth Herner 1385','\0',NULL,NULL,1276,331),(1120,'2016-05-13 02:39:17','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Manuel G. Fernandez/CN=UID:mgarcia','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,445),(1123,'2016-05-13 02:39:48','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Manuel G. Fernandez/CN=UID:mgarcia','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,445),(1126,'2016-05-13 02:40:21','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Manuel Fernandez/CN=UID:mgarcia','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,445),(1129,'2016-05-13 02:43:22','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Youngsoo Park/CN=UID:yspark1','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,448),(1132,'2016-05-13 02:43:57','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Youngsoo Park/CN=UID:yspark1','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,448),(1135,'2016-05-13 02:49:03','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Luiz alberto N. Da costa/CN=UID:ldacosta','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,451),(1138,'2016-05-13 02:49:48','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Luiz alberto N. Da costa/CN=UID:ldacosta','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,451),(1141,'2016-05-13 02:50:18','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Luiz alberto Da costa/CN=UID:ldacosta','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,451),(1144,'2016-05-13 02:56:34','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Yuanyuan Zhang/CN=UID:ynzhang','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,454),(1147,'2016-05-13 02:57:29','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Yuanyuan Zhang/CN=UID:ynzhang','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,454),(1150,'2016-05-13 02:58:02','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Yuanyuan Zhang/CN=UID:ynzhang','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,454),(1153,'2016-05-13 11:53:56','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Michel Aguena da silva/CN=UID:aguena','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,457),(1156,'2016-05-13 11:54:26','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Michel Aguena da silva/CN=UID:aguena','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,457),(1159,'2016-05-13 11:54:57','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Michel Aguena da silva/CN=UID:aguena','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,457),(1162,'2016-05-13 12:34:06','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Ignacio N. Sevilla/CN=UID:nsevilla','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,460),(1165,'2016-05-13 12:34:41','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Ignacio N. Sevilla/CN=UID:nsevilla','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,460),(1168,'2016-05-13 12:35:23','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Ignacio Sevilla/CN=UID:nsevilla','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,460),(1171,'2016-05-13 12:38:16','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Angelo Fausti neto/CN=UID:fausti','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,463),(1174,'2016-05-13 12:38:51','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Angelo Fausti neto/CN=UID:fausti','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,463),(1177,'2016-05-13 12:39:12','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Angelo Fausti neto/CN=UID:fausti','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,463),(1180,'2016-05-13 12:40:47','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Riccardo Campisano/CN=UID:rcampisa','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,466),(1183,'2016-05-13 12:41:27','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Riccardo Campisano/CN=UID:rcampisa','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,466),(1186,'2016-05-13 12:41:52','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Riccardo Campisano/CN=UID:rcampisa','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,466),(1189,'2016-05-13 12:44:29','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Carlos A. Souza/CN=UID:adean','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,469),(1192,'2016-05-13 12:45:03','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Carlos A. Souza/CN=UID:adean','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',72,469),(1195,'2016-05-13 12:46:16','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Carlos Souza/CN=UID:adean','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,469),(1222,'2016-05-18 21:17:22','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Thomas Diehl/CN=UID:diehl','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,433),(1318,'2016-05-18 21:19:18','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Youngsoo Park/CN=UID:yspark1','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,448),(1321,'2016-05-25 13:26:54','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Hallie F. Gaitsch/CN=UID:hgaitsch','\0',NULL,NULL,72,475),(1324,'2016-05-25 13:27:25','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Hallie F. Gaitsch/CN=UID:hgaitsch','\0',NULL,NULL,72,475),(1327,'2016-05-25 13:27:57','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Hallie Gaitsch/CN=UID:hgaitsch','\0',NULL,NULL,1122,475),(1333,'2016-06-07 11:25:17','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Dillon Brout/CN=UID:djbrout','\0',NULL,NULL,72,481),(1336,'2016-06-07 11:26:30','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Dillon Brout/CN=UID:djbrout','\0',NULL,NULL,1122,481),(1339,'2016-06-07 11:27:51','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Dillon Brout/CN=UID:djbrout','\0',NULL,NULL,72,481),(1342,'2016-06-13 16:41:07','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Lisa Giacchetti/CN=UID:lisa','\0',NULL,NULL,72,487),(1529,'2016-06-30 10:40:22','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Lisa Giacchetti/CN=UID:lisa','\0',NULL,NULL,1122,487),(1531,'2016-06-30 16:58:28','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Amanda Gao/CN=UID:agao','\0',NULL,NULL,1122,490),(1652,'2016-07-01 13:11:36','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Bo Jayatilaka/CN=UID:boj','\0',NULL,NULL,1122,282),(1661,'2016-07-01 13:11:52','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Flavia Sobreira/CN=UID:sobreira','\0',NULL,NULL,1122,361),(1697,'2016-07-01 13:12:17','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Michel Aguena/CN=UID:aguena','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',1122,457),(1699,'2016-07-06 17:27:19','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Timothy Osborn/CN=UID:tosborn','\0',NULL,NULL,1122,496),(1702,'2016-07-06 17:27:41','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Timothy K. Osborn/CN=UID:tosborn','\0',NULL,NULL,72,496),(1705,'2016-07-06 17:28:20','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Timothy K. Osborn/CN=UID:tosborn','\0',NULL,NULL,72,496),(1711,'2016-07-12 16:44:05','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Paul Chichura/CN=UID:pchich','\0',NULL,NULL,1122,499),(1714,'2016-07-12 16:44:34','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Paul M. Chichura/CN=UID:pchich','\0',NULL,NULL,72,499),(1717,'2016-07-12 16:45:03','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Paul M. Chichura/CN=UID:pchich','\0',NULL,NULL,72,499),(109323,'2016-08-31 12:12:00','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Dillon Brout 3823','\0',NULL,NULL,1276,481),(131430,'2016-09-07 11:16:50','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Christopher Annis/CN=UID:cannis','\0',NULL,NULL,1122,501),(131432,'2016-09-21 19:31:14','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Antonella Palmese/CN=UID:palmese','\0',NULL,NULL,1122,503),(131573,'2016-11-04 13:33:14','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Robert Butler/CN=UID:rbutler','\0',NULL,NULL,1122,509),(131575,'2016-12-02 10:26:36','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Francisco Paz-Chinchon 4084','\0',NULL,NULL,1276,511),(131577,'2016-12-06 13:40:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Yu-Ching Chen 4082','\0',NULL,NULL,1276,513),(131580,'2016-12-12 14:57:41','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Bobby Butler/CN=UID:rbutler','\0',NULL,NULL,1122,509),(131586,'2017-01-04 15:17:11','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Michael Wang/CN=UID:mwang','\0',NULL,NULL,1122,519),(131589,'2017-01-26 13:57:54','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Scott Dodelson/CN=UID:dodelson','\0',NULL,NULL,1122,525),(131592,'2017-02-08 12:59:54','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Zoheyr Doctor/CN=UID:zdoctor','\0',NULL,NULL,1122,531),(131598,'2017-03-16 09:47:01','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=Robots/CN=des51.fnal.gov/CN=cron/CN=Nikolay Kuropatkin/CN=UID:kuropat','\0',NULL,NULL,1122,302),(131601,'2017-03-31 09:25:17','New Cert','\0',NULL,NULL,1270,427);
/*!40000 ALTER TABLE `certificate` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `certificate_request`
--

DROP TABLE IF EXISTS `certificate_request`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `certificate_request` (
  `certificate` tinyblob,
  `certificateIssuer` varchar(255) NOT NULL,
  `certificateSubject` varchar(255) NOT NULL,
  `request_id` bigint(20) NOT NULL,
  PRIMARY KEY (`request_id`),
  KEY `FK47CA53E7D75D60A4` (`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `certificate_request`
--

LOCK TABLES `certificate_request` WRITE;
/*!40000 ALTER TABLE `certificate_request` DISABLE KEYS */;
INSERT INTO `certificate_request` VALUES (NULL,'/DC=org/DC=cilogon/C=US/O=CILogon/CN=CILogon Basic CA 1','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=Robots/CN=des51.fnal.gov/CN=cron/CN=Nikolay Kuropatkin/CN=UID:kuropat/CN=2787466964',228),(NULL,'/DC=com/DC=DigiCert-Grid/O=DigiCert Grid/CN=DigiCert Grid CA-1 G2','New Cert',231);
/*!40000 ALTER TABLE `certificate_request` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `group_attrs`
--

DROP TABLE IF EXISTS `group_attrs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `group_attrs` (
  `a_id` bigint(20) NOT NULL,
  `g_id` bigint(20) NOT NULL,
  `a_value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`a_id`,`g_id`),
  KEY `FK40B1A2E2566C2A8F` (`a_id`),
  KEY `FK40B1A2E2DEFC581C` (`g_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `group_attrs`
--

LOCK TABLES `group_attrs` WRITE;
/*!40000 ALTER TABLE `group_attrs` DISABLE KEYS */;
/*!40000 ALTER TABLE `group_attrs` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `group_membership_req`
--

DROP TABLE IF EXISTS `group_membership_req`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `group_membership_req` (
  `groupName` varchar(255) NOT NULL,
  `request_id` bigint(20) NOT NULL,
  PRIMARY KEY (`request_id`),
  KEY `FKBD145E75D75D60A4` (`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `group_membership_req`
--

LOCK TABLES `group_membership_req` WRITE;
/*!40000 ALTER TABLE `group_membership_req` DISABLE KEYS */;
/*!40000 ALTER TABLE `group_membership_req` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `groups`
--

DROP TABLE IF EXISTS `groups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `groups` (
  `gid` bigint(20) NOT NULL AUTO_INCREMENT,
  `dn` varchar(255) NOT NULL,
  `description` varchar(255) DEFAULT NULL,
  `parent` bigint(20) DEFAULT NULL,
  `must` bit(1) NOT NULL,
  `restricted` bit(1) DEFAULT NULL,
  PRIMARY KEY (`gid`),
  UNIQUE KEY `dn` (`dn`),
  KEY `FKB63DD9D4A3771CD3` (`parent`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `groups`
--

LOCK TABLES `groups` WRITE;
/*!40000 ALTER TABLE `groups` DISABLE KEYS */;
INSERT INTO `groups` VALUES (2,'/des',NULL,2,'',NULL),(11,'/des/production',NULL,2,'',NULL),(12,'/des/archive',NULL,2,'',NULL);
/*!40000 ALTER TABLE `groups` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `m`
--

DROP TABLE IF EXISTS `m`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `m` (
  `mapping_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `userid` bigint(20) NOT NULL,
  `gid` bigint(20) NOT NULL,
  `rid` bigint(20) DEFAULT NULL,
  `cid` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`mapping_id`),
  UNIQUE KEY `userid` (`userid`,`gid`,`rid`),
  KEY `fk_m_roles` (`rid`),
  KEY `fk_m_groups` (`gid`),
  KEY `fk_m_cap` (`cid`),
  KEY `fk_m_usr` (`userid`)
) ENGINE=InnoDB AUTO_INCREMENT=1685 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `m`
--

LOCK TABLES `m` WRITE;
/*!40000 ALTER TABLE `m` DISABLE KEYS */;
INSERT INTO `m` VALUES (21,21,2,NULL,NULL),(51,51,2,NULL,NULL),(91,91,2,NULL,NULL),(111,111,2,NULL,NULL),(121,121,2,NULL,NULL),(191,51,11,NULL,NULL),(211,91,11,NULL,NULL),(231,111,11,NULL,NULL),(241,121,11,NULL,NULL),(361,91,2,2,NULL),(371,51,11,21,NULL),(401,51,2,21,NULL),(402,142,2,NULL,NULL),(411,142,11,NULL,NULL),(422,142,12,NULL,NULL),(432,152,2,NULL,NULL),(442,152,11,NULL,NULL),(451,161,2,NULL,NULL),(461,161,11,NULL,NULL),(462,161,11,11,NULL),(471,171,2,NULL,NULL),(481,171,11,NULL,NULL),(582,182,2,NULL,NULL),(592,182,12,NULL,NULL),(602,192,2,NULL,NULL),(622,202,2,NULL,NULL),(632,202,11,NULL,NULL),(641,211,2,NULL,NULL),(652,212,2,NULL,NULL),(662,212,12,NULL,NULL),(672,212,11,NULL,NULL),(681,221,2,NULL,NULL),(691,221,12,NULL,NULL),(701,231,2,NULL,NULL),(711,231,11,NULL,NULL),(721,221,11,NULL,NULL),(731,221,11,11,NULL),(741,241,2,NULL,NULL),(772,262,2,NULL,NULL),(782,262,11,NULL,NULL),(792,272,2,NULL,NULL),(812,272,11,NULL,NULL),(822,282,2,NULL,NULL),(832,282,11,NULL,NULL),(841,291,2,NULL,NULL),(851,291,2,2,NULL),(852,292,2,NULL,NULL),(862,292,11,NULL,NULL),(871,301,2,NULL,NULL),(872,301,11,NULL,NULL),(881,192,2,2,NULL),(882,192,2,22,NULL),(922,171,11,32,NULL),(942,302,2,NULL,NULL),(952,302,2,32,NULL),(962,302,2,2,NULL),(972,302,11,NULL,NULL),(982,302,11,32,NULL),(992,302,2,42,NULL),(1001,291,2,32,NULL),(1011,91,2,32,NULL),(1021,241,2,32,NULL),(1031,192,2,32,NULL),(1051,111,2,32,NULL),(1061,301,2,32,NULL),(1071,282,2,32,NULL),(1081,171,2,32,NULL),(1082,312,2,NULL,NULL),(1092,312,2,32,NULL),(1102,322,2,NULL,NULL),(1112,322,2,32,NULL),(1122,322,2,42,NULL),(1131,322,11,NULL,NULL),(1132,322,11,32,NULL),(1142,322,11,42,NULL),(1161,171,2,2,NULL),(1171,171,11,2,NULL),(1181,331,2,NULL,NULL),(1191,331,2,32,NULL),(1201,341,2,NULL,NULL),(1211,341,2,32,NULL),(1212,211,2,32,NULL),(1221,351,2,NULL,NULL),(1231,351,2,32,NULL),(1241,361,2,NULL,NULL),(1251,361,2,32,NULL),(1252,21,2,32,NULL),(1261,371,2,NULL,NULL),(1271,381,2,NULL,NULL),(1272,381,2,32,NULL),(1282,371,2,32,NULL),(1292,382,2,NULL,NULL),(1302,382,2,42,NULL),(1311,382,11,NULL,NULL),(1312,392,2,NULL,NULL),(1321,401,2,NULL,NULL),(1331,401,12,NULL,NULL),(1333,331,2,43,NULL),(1336,211,2,43,NULL),(1339,403,2,NULL,NULL),(1342,403,2,32,NULL),(1345,403,2,42,NULL),(1348,406,2,NULL,NULL),(1351,409,2,NULL,NULL),(1353,406,12,NULL,NULL),(1356,409,12,NULL,NULL),(1359,406,11,NULL,NULL),(1362,406,11,32,NULL),(1365,406,12,32,NULL),(1368,406,2,32,NULL),(1371,409,12,32,NULL),(1374,409,2,32,NULL),(1377,409,11,NULL,NULL),(1380,409,11,32,NULL),(1381,403,11,NULL,NULL),(1383,411,2,NULL,NULL),(1386,411,2,32,NULL),(1387,412,2,NULL,NULL),(1402,212,2,42,NULL),(1408,412,2,42,NULL),(1411,412,11,NULL,NULL),(1414,412,12,NULL,NULL),(1420,412,2,32,NULL),(1423,412,11,32,NULL),(1426,212,2,32,NULL),(1429,212,11,32,NULL),(1432,415,2,NULL,NULL),(1435,415,2,32,NULL),(1437,417,2,NULL,NULL),(1443,231,2,32,NULL),(1446,231,11,32,NULL),(1449,417,11,NULL,NULL),(1452,417,11,32,NULL),(1455,417,2,32,NULL),(1458,401,2,32,NULL),(1461,401,2,42,NULL),(1464,401,11,NULL,NULL),(1467,401,11,32,NULL),(1470,415,2,43,NULL),(1473,423,2,NULL,NULL),(1476,423,2,32,NULL),(1479,423,2,43,NULL),(1483,427,2,NULL,NULL),(1486,427,11,NULL,NULL),(1489,341,2,43,NULL),(1492,433,2,NULL,NULL),(1495,433,2,32,NULL),(1501,439,2,NULL,NULL),(1504,439,2,32,NULL),(1507,442,2,NULL,NULL),(1510,442,2,32,NULL),(1513,445,2,NULL,NULL),(1516,445,2,32,NULL),(1519,448,2,NULL,NULL),(1522,448,2,32,NULL),(1525,451,2,NULL,NULL),(1528,451,2,32,NULL),(1531,454,2,NULL,NULL),(1534,454,2,32,NULL),(1537,457,2,NULL,NULL),(1540,457,2,32,NULL),(1543,460,2,NULL,NULL),(1546,460,2,32,NULL),(1549,463,2,NULL,NULL),(1552,463,2,32,NULL),(1555,466,2,NULL,NULL),(1558,466,2,32,NULL),(1561,469,2,NULL,NULL),(1564,469,2,32,NULL),(1567,475,2,NULL,NULL),(1570,475,2,32,NULL),(1576,481,2,NULL,NULL),(1579,481,2,32,NULL),(1582,487,2,NULL,NULL),(1585,490,2,NULL,NULL),(1588,490,2,32,NULL),(1591,496,2,NULL,NULL),(1594,496,2,32,NULL),(1597,496,2,43,NULL),(1600,481,2,43,NULL),(1603,499,2,NULL,NULL),(1606,499,2,32,NULL),(1609,499,2,43,NULL),(1612,361,2,43,NULL),(1614,331,2,2,NULL),(1620,501,2,NULL,NULL),(1623,501,2,32,NULL),(1625,503,2,NULL,NULL),(1628,503,2,32,NULL),(1634,509,2,NULL,NULL),(1637,509,2,32,NULL),(1640,509,2,43,NULL),(1642,511,2,NULL,NULL),(1644,511,11,NULL,NULL),(1650,513,2,NULL,NULL),(1656,513,11,NULL,NULL),(1662,519,2,NULL,NULL),(1665,519,2,32,NULL),(1668,525,2,NULL,NULL),(1671,525,2,32,NULL),(1674,531,2,NULL,NULL),(1677,531,2,32,NULL),(1680,302,11,43,NULL),(1682,487,2,32,NULL);
/*!40000 ALTER TABLE `m` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `memb_req`
--

DROP TABLE IF EXISTS `memb_req`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `memb_req` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `creation_date` datetime NOT NULL,
  `evaluation_date` datetime DEFAULT NULL,
  `status` int(11) NOT NULL,
  `confirm_id` varchar(255) NOT NULL,
  `dn` varchar(255) NOT NULL,
  `ca` varchar(255) NOT NULL,
  `cn` varchar(255) DEFAULT NULL,
  `mail` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `memb_req`
--

LOCK TABLES `memb_req` WRITE;
/*!40000 ALTER TABLE `memb_req` DISABLE KEYS */;
/*!40000 ALTER TABLE `memb_req` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `membership_rem_req`
--

DROP TABLE IF EXISTS `membership_rem_req`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `membership_rem_req` (
  `reason` varchar(255) NOT NULL,
  `request_id` bigint(20) NOT NULL,
  PRIMARY KEY (`request_id`),
  KEY `FK1877BC10D75D60A4` (`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `membership_rem_req`
--

LOCK TABLES `membership_rem_req` WRITE;
/*!40000 ALTER TABLE `membership_rem_req` DISABLE KEYS */;
/*!40000 ALTER TABLE `membership_rem_req` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `personal_info`
--

DROP TABLE IF EXISTS `personal_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `personal_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `value` varchar(255) DEFAULT NULL,
  `visible` bit(1) DEFAULT NULL,
  `personal_info_type_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `FK229FDF4DA8D3C6BC` (`personal_info_type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `personal_info`
--

LOCK TABLES `personal_info` WRITE;
/*!40000 ALTER TABLE `personal_info` DISABLE KEYS */;
/*!40000 ALTER TABLE `personal_info` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `personal_info_type`
--

DROP TABLE IF EXISTS `personal_info_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `personal_info_type` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `description` varchar(255) DEFAULT NULL,
  `type` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `type` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `personal_info_type`
--

LOCK TABLES `personal_info_type` WRITE;
/*!40000 ALTER TABLE `personal_info_type` DISABLE KEYS */;
/*!40000 ALTER TABLE `personal_info_type` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `req`
--

DROP TABLE IF EXISTS `req`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `req` (
  `request_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `completionDate` datetime DEFAULT NULL,
  `creationDate` datetime DEFAULT NULL,
  `expirationDate` datetime DEFAULT NULL,
  `status` varchar(255) NOT NULL,
  `requester_info_id` bigint(20) NOT NULL,
  PRIMARY KEY (`request_id`),
  UNIQUE KEY `requester_info_id` (`requester_info_id`),
  KEY `FK1B89EC37E889D` (`requester_info_id`)
) ENGINE=InnoDB AUTO_INCREMENT=232 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `req`
--

LOCK TABLES `req` WRITE;
/*!40000 ALTER TABLE `req` DISABLE KEYS */;
INSERT INTO `req` VALUES (219,'2017-01-04 15:17:11','2017-01-04 15:13:59','2017-01-11 15:13:59','APPROVED',222),(225,'2017-01-26 13:57:54','2017-01-26 13:56:39','2017-02-02 13:56:39','APPROVED',228),(228,'2017-03-16 09:48:56','2017-03-16 09:21:53','2017-03-23 09:21:53','APPROVED',231),(231,'2017-03-31 09:25:17','2017-03-31 09:24:44','2017-04-07 09:24:44','APPROVED',234);
/*!40000 ALTER TABLE `req` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `requester_info`
--

DROP TABLE IF EXISTS `requester_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `requester_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `address` varchar(255) DEFAULT NULL,
  `certificateIssuer` varchar(255) NOT NULL,
  `certificateSubject` varchar(255) NOT NULL,
  `emailAddress` varchar(255) NOT NULL,
  `institution` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `phoneNumber` varchar(255) DEFAULT NULL,
  `surname` varchar(255) DEFAULT NULL,
  `voMember` bit(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=235 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `requester_info`
--

LOCK TABLES `requester_info` WRITE;
/*!40000 ALTER TABLE `requester_info` DISABLE KEYS */;
INSERT INTO `requester_info` VALUES (222,'Fermilab\r\nKirk Rd. at Pine St.\r\nBatavia, IL 60510','/DC=org/DC=cilogon/C=US/O=CILogon/CN=CILogon Basic CA 1','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Michael Wang/CN=UID:mwang','mwang@fnal.gov','Fermilab','Michael','(630)840-2947','Wang',NULL),(228,'Fermilab\r\nPO Box 500','/DC=org/DC=cilogon/C=US/O=CILogon/CN=CILogon Basic CA 1','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Scott Dodelson/CN=UID:dodelson','dodelson@fnal.gov','Fermilab','Scott','6308402426','Dodelson',NULL),(231,'Kirk Rd Pine Street','/DC=org/DC=cilogon/C=US/O=CILogon/CN=CILogon OSG CA 1','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Nikolay Kuropatkin 51','kuropat@fnal.gov','Fermilab','Nikolay','6308402416','Kuropatkin',''),(234,'1205 W. Clark St., Urbana, IL 61801','/DC=com/DC=DigiCert-Grid/O=DigiCert Grid/CN=DigiCert Grid CA-1 G2','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Michael Johnson 2274','mjohns44@illinois.edu','National Center for Supercomputing Applications','Michael','217-300-0193','Johnson','');
/*!40000 ALTER TABLE `requester_info` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `requester_personal_info`
--

DROP TABLE IF EXISTS `requester_personal_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `requester_personal_info` (
  `requester_id` bigint(20) NOT NULL,
  `pi_value` varchar(255) DEFAULT NULL,
  `pi_key` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`requester_id`,`pi_key`),
  KEY `FK7E3D7FCAD500B8D2` (`requester_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `requester_personal_info`
--

LOCK TABLES `requester_personal_info` WRITE;
/*!40000 ALTER TABLE `requester_personal_info` DISABLE KEYS */;
/*!40000 ALTER TABLE `requester_personal_info` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `role_attrs`
--

DROP TABLE IF EXISTS `role_attrs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `role_attrs` (
  `a_id` bigint(20) NOT NULL,
  `g_id` bigint(20) NOT NULL,
  `r_id` bigint(20) NOT NULL,
  `a_value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`a_id`,`g_id`,`r_id`),
  KEY `FK6BDE9799566C2A8F` (`a_id`),
  KEY `FK6BDE979920304994` (`r_id`),
  KEY `FK6BDE9799DEFC581C` (`g_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `role_attrs`
--

LOCK TABLES `role_attrs` WRITE;
/*!40000 ALTER TABLE `role_attrs` DISABLE KEYS */;
/*!40000 ALTER TABLE `role_attrs` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `role_membership_req`
--

DROP TABLE IF EXISTS `role_membership_req`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `role_membership_req` (
  `groupName` varchar(255) DEFAULT NULL,
  `roleName` varchar(255) DEFAULT NULL,
  `request_id` bigint(20) NOT NULL,
  PRIMARY KEY (`request_id`),
  KEY `FK3B9C79ED75D60A4` (`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `role_membership_req`
--

LOCK TABLES `role_membership_req` WRITE;
/*!40000 ALTER TABLE `role_membership_req` DISABLE KEYS */;
/*!40000 ALTER TABLE `role_membership_req` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `roles`
--

DROP TABLE IF EXISTS `roles`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `roles` (
  `rid` bigint(20) NOT NULL AUTO_INCREMENT,
  `role` varchar(255) NOT NULL,
  PRIMARY KEY (`rid`),
  UNIQUE KEY `role` (`role`)
) ENGINE=InnoDB AUTO_INCREMENT=44 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `roles`
--

LOCK TABLES `roles` WRITE;
/*!40000 ALTER TABLE `roles` DISABLE KEYS */;
INSERT INTO `roles` VALUES (32,'Analysis'),(43,'DESGW'),(22,'pilot'),(42,'Production'),(11,'root'),(2,'VO-Admin'),(21,'VOMS-Query');
/*!40000 ALTER TABLE `roles` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `seqnumber`
--

DROP TABLE IF EXISTS `seqnumber`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `seqnumber` (
  `seq` varchar(255) NOT NULL,
  PRIMARY KEY (`seq`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `seqnumber`
--

LOCK TABLES `seqnumber` WRITE;
/*!40000 ALTER TABLE `seqnumber` DISABLE KEYS */;
INSERT INTO `seqnumber` VALUES ('0');
/*!40000 ALTER TABLE `seqnumber` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sign_aup_task`
--

DROP TABLE IF EXISTS `sign_aup_task`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sign_aup_task` (
  `task_id` bigint(20) NOT NULL,
  `aup_id` bigint(20) NOT NULL,
  PRIMARY KEY (`task_id`),
  KEY `FK7FCB416A32B8C70C` (`task_id`),
  KEY `FK7FCB416ADA1C6363` (`aup_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sign_aup_task`
--

LOCK TABLES `sign_aup_task` WRITE;
/*!40000 ALTER TABLE `sign_aup_task` DISABLE KEYS */;
INSERT INTO `sign_aup_task` VALUES (12,2),(32,2),(62,2),(82,2),(92,2),(121,2),(141,2),(151,2),(161,2),(171,2),(181,2),(191,2),(201,2),(221,2),(242,2),(252,2),(262,2),(272,2),(282,2),(291,2),(302,2),(311,2),(312,2),(331,2),(341,2),(342,2),(351,2),(361,2),(371,2),(381,2),(391,2),(394,2),(397,2),(400,2),(403,2),(405,2),(406,2),(408,2),(412,2),(415,2),(418,2),(421,2),(425,2),(427,2),(431,2),(434,2),(437,2),(439,2),(440,2),(442,2),(445,2),(451,2),(453,2),(459,2),(461,2),(464,2),(467,2),(470,2),(476,2),(482,2),(484,2),(487,2),(489,2),(490,2),(495,2),(496,2),(501,2),(505,2),(508,2),(514,2),(516,2),(519,2),(525,2),(531,2),(537,2),(540,2),(543,2),(546,2),(549,2),(552,2),(555,2),(561,2),(564,2),(567,2),(570,2),(573,2),(575,2);
/*!40000 ALTER TABLE `sign_aup_task` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tag_mapping`
--

DROP TABLE IF EXISTS `tag_mapping`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tag_mapping` (
  `mapping_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `tag_id` bigint(20) NOT NULL,
  `gid` bigint(20) NOT NULL,
  `rid` bigint(20) DEFAULT NULL,
  `admin_id` bigint(20) NOT NULL,
  PRIMARY KEY (`mapping_id`),
  UNIQUE KEY `tag_id` (`tag_id`,`gid`,`rid`,`admin_id`),
  KEY `fk_tag_mapping_roles` (`rid`),
  KEY `fk_tag_mapping_admins` (`admin_id`),
  KEY `fk_tag_mapping_groups` (`gid`),
  KEY `fk_tag_mapping_tag` (`tag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tag_mapping`
--

LOCK TABLES `tag_mapping` WRITE;
/*!40000 ALTER TABLE `tag_mapping` DISABLE KEYS */;
/*!40000 ALTER TABLE `tag_mapping` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `tags`
--

DROP TABLE IF EXISTS `tags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tags` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `implicit` bit(1) NOT NULL,
  `permissions` int(11) NOT NULL,
  `permissionsOnPath` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `tags`
--

LOCK TABLES `tags` WRITE;
/*!40000 ALTER TABLE `tags` DISABLE KEYS */;
/*!40000 ALTER TABLE `tags` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `task`
--

DROP TABLE IF EXISTS `task`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `task` (
  `task_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `completionDate` datetime DEFAULT NULL,
  `creationDate` datetime DEFAULT NULL,
  `expiryDate` datetime DEFAULT NULL,
  `status` varchar(255) NOT NULL,
  `admin_id` bigint(20) DEFAULT NULL,
  `task_type_id` bigint(20) NOT NULL,
  `usr_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`task_id`),
  KEY `FK3635859AD54C57` (`task_type_id`),
  KEY `FK363585EE2D4487` (`usr_id`),
  KEY `FK363585A4AD9904` (`admin_id`)
) ENGINE=InnoDB AUTO_INCREMENT=578 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `task`
--

LOCK TABLES `task` WRITE;
/*!40000 ALTER TABLE `task` DISABLE KEYS */;
INSERT INTO `task` VALUES (12,'2013-01-29 13:02:15','2013-01-29 13:01:01','2013-01-30 13:01:01','COMPLETED',NULL,2,21),(32,'2013-01-30 02:45:12','2013-01-29 13:01:01','2013-01-30 13:01:01','COMPLETED',NULL,2,51),(62,'2013-01-29 14:07:25','2013-01-29 13:01:01','2013-01-30 13:01:01','COMPLETED',NULL,2,91),(82,'2013-01-29 17:01:41','2013-01-29 13:01:01','2013-01-30 13:01:01','COMPLETED',NULL,2,111),(92,'2013-01-29 17:11:25','2013-01-29 13:01:01','2013-01-30 13:01:01','COMPLETED',NULL,2,121),(121,'2015-06-09 10:43:19','2014-01-30 00:00:42','2014-01-31 00:00:42','COMPLETED',NULL,2,21),(141,'2014-01-31 08:22:00','2014-01-30 00:00:43','2014-01-31 00:00:43','COMPLETED',NULL,2,91),(151,'2014-02-14 09:56:34','2014-01-30 00:00:43','2014-01-31 00:00:43','COMPLETED',NULL,2,111),(161,'2016-05-02 16:41:46','2014-01-30 00:00:43','2014-01-31 00:00:43','COMPLETED',NULL,2,121),(171,NULL,'2014-01-30 00:00:43','2014-01-31 00:00:43','EXPIRED',NULL,2,142),(181,NULL,'2014-01-30 00:00:43','2014-01-31 00:00:43','EXPIRED',NULL,2,152),(191,NULL,'2014-01-30 00:00:43','2014-01-31 00:00:43','EXPIRED',NULL,2,161),(201,'2014-01-30 08:26:16','2014-01-30 00:00:43','2014-01-31 00:00:43','COMPLETED',NULL,2,171),(221,NULL,'2014-01-30 02:45:42','2014-01-31 02:45:42','EXPIRED',NULL,2,51),(242,NULL,'2014-04-10 12:42:31','2014-04-11 12:42:31','EXPIRED',NULL,2,182),(252,'2014-12-19 10:11:49','2014-07-22 09:53:47','2014-07-23 09:53:47','COMPLETED',NULL,2,192),(262,'2014-09-22 16:12:40','2014-09-05 12:14:35','2014-09-20 12:14:35','COMPLETED',NULL,2,211),(272,'2014-09-29 11:18:35','2014-09-27 12:35:34','2014-10-12 12:35:33','COMPLETED',NULL,2,221),(282,'2014-09-29 08:29:44','2014-09-27 12:45:33','2014-10-12 12:45:33','COMPLETED',NULL,2,231),(291,'2015-02-14 13:44:44','2015-01-30 08:27:18','2015-02-14 08:27:18','COMPLETED',NULL,2,171),(302,'2015-01-30 09:39:48','2015-01-30 09:37:18','2015-02-14 09:37:18','COMPLETED',NULL,2,91),(311,'2015-02-02 09:44:45','2015-01-31 10:17:18','2015-02-15 10:17:18','COMPLETED',NULL,2,241),(312,NULL,'2015-01-31 11:19:42','2015-02-15 11:19:42','EXPIRED',NULL,2,202),(331,'2015-02-15 18:17:26','2015-01-31 14:37:18','2015-02-15 14:37:18','COMPLETED',NULL,2,212),(341,'2015-02-13 13:08:08','2015-02-03 10:37:18','2015-02-18 10:37:18','COMPLETED',NULL,2,262),(342,NULL,'2015-02-12 15:03:33','2015-02-27 15:03:32','EXPIRED',NULL,2,272),(351,NULL,'2015-02-13 13:11:04','2015-02-28 13:11:04','EXPIRED',NULL,2,282),(361,NULL,'2015-02-14 09:58:32','2015-03-01 09:58:32','EXPIRED',NULL,2,111),(371,'2015-05-01 10:42:40','2015-04-16 10:33:04','2015-05-01 10:33:04','COMPLETED',NULL,2,291),(381,NULL,'2015-06-24 11:23:35','2015-07-09 11:23:35','EXPIRED',NULL,2,292),(391,NULL,'2015-08-04 09:13:18','2015-08-19 09:13:18','EXPIRED',NULL,2,301),(394,'2015-09-22 16:48:31','2015-09-22 16:18:29','2015-10-07 16:18:29','COMPLETED',NULL,2,211),(397,'2015-10-14 09:46:32','2015-09-29 08:30:06','2015-10-14 08:30:06','COMPLETED',NULL,2,231),(400,'2015-09-30 09:31:13','2015-09-29 11:20:06','2015-10-14 11:20:06','COMPLETED',NULL,2,221),(403,'2015-12-21 09:41:26','2015-12-19 10:13:00','2016-01-03 10:13:00','COMPLETED',NULL,2,192),(405,'2016-01-25 08:53:37','2016-01-09 14:24:55','2016-01-24 14:24:55','COMPLETED',NULL,2,302),(406,'2016-02-03 09:44:20','2016-01-30 09:40:24','2016-02-14 09:40:24','COMPLETED',NULL,2,91),(408,'2016-02-02 12:05:06','2016-02-02 09:49:30','2016-02-17 09:49:30','COMPLETED',NULL,2,241),(412,'2016-02-28 14:46:46','2016-02-13 13:10:24','2016-02-28 13:10:24','COMPLETED',NULL,2,262),(415,'2016-02-14 14:49:00','2016-02-14 13:50:24','2016-02-29 13:50:24','COMPLETED',NULL,2,171),(418,'2016-03-01 18:25:49','2016-02-15 18:20:24','2016-03-01 18:20:24','COMPLETED',NULL,2,212),(421,'2016-04-01 17:08:04','2016-03-03 17:00:24','2016-03-18 17:00:24','COMPLETED',NULL,2,312),(425,NULL,'2016-04-06 13:16:46','2016-04-21 13:16:46','EXPIRED',NULL,2,322),(427,NULL,'2016-04-30 10:47:16','2016-05-15 10:47:16','EXPIRED',NULL,2,291),(431,'2016-05-12 18:19:24','2016-05-04 11:41:01','2016-05-19 11:41:01','COMPLETED',NULL,2,331),(434,'2016-06-02 15:21:49','2016-05-18 14:16:12','2016-06-02 14:16:12','COMPLETED',NULL,2,341),(437,'2016-06-16 03:52:14','2016-05-31 11:53:42','2016-06-15 11:53:42','COMPLETED',NULL,2,351),(439,'2016-06-23 15:42:24','2016-06-04 11:57:30','2016-06-19 11:57:30','COMPLETED',NULL,2,361),(440,'2016-06-08 11:27:17','2016-06-08 10:48:13','2016-06-23 10:48:13','COMPLETED',NULL,2,21),(442,'2016-06-23 14:52:02','2016-06-08 14:49:37','2016-06-23 14:49:37','COMPLETED',NULL,2,371),(445,NULL,'2016-06-08 14:59:37','2016-06-23 14:59:37','EXPIRED',NULL,2,381),(451,'2016-07-21 10:52:04','2016-07-06 10:40:50','2016-07-21 10:40:50','COMPLETED',NULL,2,382),(453,NULL,'2016-07-23 15:40:26','2016-08-07 15:40:26','EXPIRED',NULL,2,392),(459,'2016-07-29 08:48:22','2016-07-28 15:27:33','2016-08-12 15:27:33','COMPLETED',NULL,2,401),(461,NULL,'2016-09-13 10:52:39','2016-09-28 10:52:39','EXPIRED',NULL,2,403),(464,NULL,'2016-09-17 20:05:52','2016-10-02 20:05:52','EXPIRED',NULL,2,406),(467,NULL,'2016-09-17 20:05:52','2016-10-02 20:05:52','EXPIRED',NULL,2,409),(470,'2016-10-06 16:59:32','2016-09-21 16:55:51','2016-10-06 16:55:51','COMPLETED',NULL,2,211),(476,'2016-09-29 09:38:07','2016-09-29 09:35:51','2016-10-14 09:35:51','COMPLETED',NULL,2,221),(482,'2016-10-11 13:15:21','2016-10-11 10:16:39','2016-10-26 10:16:39','COMPLETED',NULL,2,411),(484,NULL,'2016-12-20 09:49:00','2017-01-04 09:49:00','EXPIRED',NULL,2,192),(487,'2016-12-21 16:07:31','2016-12-21 14:19:00','2017-01-05 14:19:00','COMPLETED',NULL,2,415),(489,'2017-01-26 22:57:34','2017-01-21 14:37:18','2017-02-05 14:37:18','COMPLETED',NULL,2,231),(490,NULL,'2017-01-28 14:04:48','2017-02-12 14:04:48','EXPIRED',NULL,2,417),(495,NULL,'2017-02-01 12:07:18','2017-02-16 12:07:18','EXPIRED',NULL,2,241),(496,'2017-02-02 09:50:40','2017-02-02 09:44:48','2017-02-17 09:44:48','COMPLETED',NULL,2,91),(501,'2017-02-18 08:57:33','2017-02-13 14:53:30','2017-02-28 14:53:30','COMPLETED',NULL,2,171),(505,'2017-03-13 15:06:14','2017-03-01 18:26:38','2017-03-16 18:26:38','COMPLETED',NULL,2,212),(508,'2017-03-16 20:19:44','2017-03-01 19:53:31','2017-03-16 19:53:31','COMPLETED',NULL,2,412),(514,'2017-03-31 09:44:15','2017-03-02 13:16:38','2017-03-17 13:16:38','COMPLETED',NULL,2,262),(516,NULL,'2017-03-15 14:20:07','2017-03-30 14:20:07','EXPIRED',NULL,2,423),(519,NULL,'2017-04-01 17:10:07','2017-04-16 17:10:07','EXPIRED',NULL,2,312),(525,NULL,'2017-04-14 15:12:19','2017-04-29 15:12:19','EXPIRED',NULL,2,433),(531,NULL,'2017-05-02 16:42:19','2017-05-17 16:42:19','EXPIRED',NULL,2,121),(537,'2017-05-12 18:23:38','2017-05-12 18:22:19','2017-05-27 18:22:19','COMPLETED',NULL,2,331),(540,NULL,'2017-05-13 02:17:32','2017-05-28 02:17:32','EXPIRED',NULL,2,439),(543,NULL,'2017-05-13 02:32:19','2017-05-28 02:32:19','EXPIRED',NULL,2,442),(546,NULL,'2017-05-13 02:42:19','2017-05-28 02:42:19','EXPIRED',NULL,2,445),(549,NULL,'2017-05-13 02:47:32','2017-05-28 02:47:32','EXPIRED',NULL,2,448),(552,NULL,'2017-05-13 02:52:19','2017-05-28 02:52:19','EXPIRED',NULL,2,451),(555,NULL,'2017-05-13 02:57:32','2017-05-28 02:57:32','EXPIRED',NULL,2,454),(561,NULL,'2017-05-13 11:57:32','2017-05-28 11:57:32','EXPIRED',NULL,2,457),(564,NULL,'2017-05-13 12:37:32','2017-05-28 12:37:32','EXPIRED',NULL,2,460),(567,NULL,'2017-05-13 12:42:19','2017-05-28 12:42:19','EXPIRED',NULL,2,463),(570,NULL,'2017-05-13 12:42:19','2017-05-28 12:42:19','EXPIRED',NULL,2,466),(573,NULL,'2017-05-13 12:47:33','2017-05-28 12:47:33','EXPIRED',NULL,2,469),(575,NULL,'2017-05-25 13:29:46','2017-06-09 13:29:46','CREATED',NULL,2,475);
/*!40000 ALTER TABLE `task` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `task_log_record`
--

DROP TABLE IF EXISTS `task_log_record`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `task_log_record` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `adminDn` varchar(255) DEFAULT NULL,
  `creation_time` datetime NOT NULL,
  `event` varchar(255) NOT NULL,
  `userDn` varchar(255) DEFAULT NULL,
  `task_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `FK77673CA632B8C70C` (`task_id`)
) ENGINE=InnoDB AUTO_INCREMENT=950 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `task_log_record`
--

LOCK TABLES `task_log_record` WRITE;
/*!40000 ALTER TABLE `task_log_record` DISABLE KEYS */;
INSERT INTO `task_log_record` VALUES (12,NULL,'2013-01-29 13:01:01','CREATED',NULL,12),(32,NULL,'2013-01-29 13:01:01','CREATED',NULL,32),(62,NULL,'2013-01-29 13:01:01','CREATED',NULL,62),(82,NULL,'2013-01-29 13:01:01','CREATED',NULL,82),(92,NULL,'2013-01-29 13:01:01','CREATED',NULL,92),(122,NULL,'2013-01-29 13:02:15','COMPLETED','/DC=org/DC=doegrids/OU=People/CN=Stephen Kent 93092',12),(152,NULL,'2013-01-29 14:07:25','COMPLETED','/DC=org/DC=doegrids/OU=People/CN=Steven Timm 74183',62),(162,NULL,'2013-01-29 17:01:41','COMPLETED','/DC=org/DC=doegrids/OU=People/CN=Gabriele Garzoglio 762243',82),(172,NULL,'2013-01-29 17:11:25','COMPLETED','/DC=org/DC=doegrids/OU=People/CN=Marko Slyz 664315',92),(182,NULL,'2013-01-30 02:45:12','COMPLETED','/DC=es/DC=irisgrid/O=pic/CN=christian.neissner',32),(221,NULL,'2014-01-30 00:00:42','CREATED',NULL,121),(241,NULL,'2014-01-30 00:00:43','CREATED',NULL,141),(251,NULL,'2014-01-30 00:00:43','CREATED',NULL,151),(261,NULL,'2014-01-30 00:00:43','CREATED',NULL,161),(271,NULL,'2014-01-30 00:00:43','CREATED',NULL,171),(281,NULL,'2014-01-30 00:00:43','CREATED',NULL,181),(291,NULL,'2014-01-30 00:00:43','CREATED',NULL,191),(301,NULL,'2014-01-30 00:00:43','CREATED',NULL,201),(321,NULL,'2014-01-30 02:45:42','CREATED',NULL,221),(331,NULL,'2014-01-30 08:26:16','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Brian P. Yanny/CN=UID:yanny',201),(362,NULL,'2014-01-31 08:22:00','COMPLETED','/DC=org/DC=doegrids/OU=People/CN=Steven Timm 74183',141),(381,NULL,'2014-02-14 09:56:34','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Gabriele Garzoglio/CN=UID:garzogli',151),(382,NULL,'2014-04-10 12:42:31','CREATED',NULL,242),(392,NULL,'2014-07-22 09:53:47','CREATED',NULL,252),(402,NULL,'2014-09-05 12:14:35','CREATED',NULL,262),(411,NULL,'2014-09-22 16:12:40','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Marcelle Soares-Santos 1836',262),(412,NULL,'2014-09-27 12:35:34','CREATED',NULL,272),(422,NULL,'2014-09-27 12:45:33','CREATED',NULL,282),(432,NULL,'2014-09-29 08:29:44','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Karl Drlica-Wagner 1914',282),(441,NULL,'2014-09-29 11:18:35','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Michelle Gower 1913',272),(451,NULL,'2014-12-19 10:11:49','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Joe B. Boyd/CN=UID:boyd',252),(461,NULL,'2015-01-30 08:27:18','CREATED',NULL,291),(472,NULL,'2015-01-30 09:37:18','CREATED',NULL,302),(482,NULL,'2015-01-30 09:39:48','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Steven C. Timm/CN=UID:timm',302),(491,NULL,'2015-01-31 10:17:18','CREATED',NULL,311),(492,NULL,'2015-01-31 11:19:42','CREATED',NULL,312),(511,NULL,'2015-01-31 14:37:18','CREATED',NULL,331),(531,NULL,'2015-02-02 09:44:45','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=gpsn01.fnal.gov/CN=cron/CN=Gerard Bernabeu Altayo/CN=UID:gerard1',311),(541,NULL,'2015-02-03 10:37:18','CREATED',NULL,341),(542,NULL,'2015-02-12 15:03:33','CREATED',NULL,342),(551,NULL,'2015-02-13 13:08:08','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Michael Johnson 2274',341),(561,NULL,'2015-02-13 13:11:04','CREATED',NULL,351),(571,NULL,'2015-02-14 09:58:32','CREATED',NULL,361),(581,NULL,'2015-02-14 13:44:44','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Brian P. Yanny/CN=UID:yanny',291),(582,NULL,'2015-02-15 18:17:26','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Greg Daues 1912',331),(591,NULL,'2015-04-16 10:33:04','CREATED',NULL,371),(592,NULL,'2015-05-01 10:42:40','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Nicholas Peregonow/CN=UID:njp',371),(601,NULL,'2015-06-09 10:43:19','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Stephen M. Kent/CN=UID:skent',121),(621,NULL,'2015-06-24 11:23:35','CREATED',NULL,381),(631,NULL,'2015-08-04 09:13:18','CREATED',NULL,391),(634,NULL,'2015-09-22 16:18:29','CREATED',NULL,394),(637,NULL,'2015-09-22 16:48:31','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Marcelle S. Santos/CN=UID:marcelle',394),(640,NULL,'2015-09-29 08:30:06','CREATED',NULL,397),(643,NULL,'2015-09-29 11:20:06','CREATED',NULL,400),(646,NULL,'2015-09-30 09:31:13','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Michelle Gower 1913',400),(649,NULL,'2015-10-14 09:46:32','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Karl Drlica-Wagner 1914',397),(652,NULL,'2015-12-19 10:13:00','CREATED',NULL,403),(655,NULL,'2015-12-21 09:41:26','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Joe B. Boyd/CN=UID:boyd',403),(657,NULL,'2016-01-09 14:24:55','CREATED',NULL,405),(660,NULL,'2016-01-25 08:53:37','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Nikolay Kuropatkin 51',405),(661,NULL,'2016-01-30 09:40:24','CREATED',NULL,406),(663,NULL,'2016-02-02 09:49:30','CREATED',NULL,408),(669,NULL,'2016-02-02 12:05:06','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=gpsn01.fnal.gov/CN=cron/CN=Gerard Bernabeu Altayo/CN=UID:gerard1',408),(670,NULL,'2016-02-03 09:44:20','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Steven C. Timm/CN=UID:timm',406),(673,NULL,'2016-02-13 13:10:24','CREATED',NULL,412),(676,NULL,'2016-02-14 13:50:24','CREATED',NULL,415),(679,NULL,'2016-02-14 14:49:00','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Brian P. Yanny/CN=UID:yanny',415),(682,NULL,'2016-02-15 18:20:24','CREATED',NULL,418),(684,NULL,'2016-02-28 14:46:46','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Michael Johnson 2274',412),(687,NULL,'2016-03-01 18:25:49','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Greg Daues 1912',418),(688,NULL,'2016-03-03 17:00:24','CREATED',NULL,421),(691,NULL,'2016-04-01 17:08:04','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Dennis D. Box/CN=UID:dbox',421),(695,NULL,'2016-04-06 13:16:46','CREATED',NULL,425),(697,NULL,'2016-04-30 10:47:16','CREATED',NULL,427),(703,NULL,'2016-05-02 16:41:46','COMPLETED','/DC=org/DC=doegrids/OU=People/CN=Marko Slyz 664315',161),(707,NULL,'2016-05-04 11:41:01','CREATED',NULL,431),(709,NULL,'2016-05-12 18:19:24','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=Services/CN=desgw/des41.fnal.gov',431),(713,NULL,'2016-05-18 14:16:12','CREATED',NULL,434),(719,NULL,'2016-05-31 11:53:42','CREATED',NULL,437),(721,NULL,'2016-06-02 15:21:49','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Masao Sako/CN=UID:masao',434),(727,NULL,'2016-06-04 11:57:30','CREATED',NULL,439),(728,NULL,'2016-06-08 10:48:13','CREATED',NULL,440),(731,NULL,'2016-06-08 11:27:17','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Stephen M. Kent/CN=UID:skent',440),(733,NULL,'2016-06-08 14:49:37','CREATED',NULL,442),(736,NULL,'2016-06-08 14:59:37','CREATED',NULL,445),(742,NULL,'2016-06-16 03:52:14','COMPLETED','/C=UK/O=eScience/OU=Manchester/L=HEP/CN=joe zuntz',437),(748,NULL,'2016-06-23 14:52:02','COMPLETED','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Huan Lin/CN=UID:hlin',442),(751,NULL,'2016-06-23 15:42:24','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Flavia Sobreira sanchez/CN=UID:sobreira',439),(757,NULL,'2016-07-06 10:40:50','CREATED',NULL,451),(759,NULL,'2016-07-21 10:52:04','COMPLETED','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Eric Morganson 3317',451),(765,NULL,'2016-07-23 15:40:26','CREATED',NULL,453),(771,NULL,'2016-07-28 15:27:33','CREATED',NULL,459),(777,NULL,'2016-07-29 08:48:22','COMPLETED','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Douglas Nathaniel Friedel 3371',459),(779,NULL,'2016-09-13 10:52:39','CREATED',NULL,461),(782,NULL,'2016-09-17 20:05:52','CREATED',NULL,464),(785,NULL,'2016-09-17 20:05:52','CREATED',NULL,467),(791,NULL,'2016-09-21 16:55:51','CREATED',NULL,470),(797,NULL,'2016-09-29 09:35:51','CREATED',NULL,476),(800,NULL,'2016-09-29 09:38:07','COMPLETED','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Michelle Gower 1913',476),(806,NULL,'2016-10-06 16:59:32','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=Marcelle S. Santos/CN=UID:marcelle',470),(812,NULL,'2016-10-11 10:16:39','CREATED',NULL,482),(815,NULL,'2016-10-11 13:15:21','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=People/CN=Eric H. NeilsenJr./CN=UID:neilsen',482),(817,NULL,'2016-12-20 09:49:00','CREATED',NULL,484),(820,NULL,'2016-12-21 14:19:00','CREATED',NULL,487),(823,NULL,'2016-12-21 16:07:31','COMPLETED','/DC=gov/DC=fnal/O=Fermilab/OU=Robots/CN=fifegrid/CN=batch/CN=James Annis/CN=UID:annis',487),(825,NULL,'2017-01-21 14:37:18','CREATED',NULL,489),(831,NULL,'2017-01-26 22:57:34','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Karl Drlica-Wagner 1914',489),(835,NULL,'2017-01-28 14:04:48','CREATED',NULL,490),(837,NULL,'2017-02-01 12:07:18','CREATED',NULL,495),(838,NULL,'2017-02-02 09:44:48','CREATED',NULL,496),(841,NULL,'2017-02-02 09:50:40','COMPLETED','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Steve Timm 167',496),(843,NULL,'2017-02-13 14:53:30','CREATED',NULL,501),(847,NULL,'2017-02-18 08:57:33','COMPLETED','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Brian Yanny/CN=UID:yanny',501),(853,NULL,'2017-03-01 18:26:38','CREATED',NULL,505),(856,NULL,'2017-03-01 19:53:31','CREATED',NULL,508),(862,NULL,'2017-03-02 13:16:38','CREATED',NULL,514),(868,NULL,'2017-03-13 15:06:14','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Greg Daues 1912',505),(870,NULL,'2017-03-15 14:20:07','CREATED',NULL,516),(876,NULL,'2017-03-16 20:19:44','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Greg Daues 1912',508),(879,NULL,'2017-03-31 09:44:15','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Michael Johnson 2274',514),(885,NULL,'2017-04-01 17:10:07','CREATED',NULL,519),(891,NULL,'2017-04-14 15:12:19','CREATED',NULL,525),(897,NULL,'2017-05-02 16:42:19','CREATED',NULL,531),(903,NULL,'2017-05-12 18:22:19','CREATED',NULL,537),(906,NULL,'2017-05-12 18:23:38','COMPLETED','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=Services/CN=desgw/des41.fnal.gov',537),(912,NULL,'2017-05-13 02:17:32','CREATED',NULL,540),(915,NULL,'2017-05-13 02:32:19','CREATED',NULL,543),(918,NULL,'2017-05-13 02:42:19','CREATED',NULL,546),(921,NULL,'2017-05-13 02:47:32','CREATED',NULL,549),(924,NULL,'2017-05-13 02:52:19','CREATED',NULL,552),(927,NULL,'2017-05-13 02:57:32','CREATED',NULL,555),(933,NULL,'2017-05-13 11:57:32','CREATED',NULL,561),(936,NULL,'2017-05-13 12:37:32','CREATED',NULL,564),(939,NULL,'2017-05-13 12:42:19','CREATED',NULL,567),(942,NULL,'2017-05-13 12:42:19','CREATED',NULL,570),(945,NULL,'2017-05-13 12:47:33','CREATED',NULL,573),(947,NULL,'2017-05-25 13:29:46','CREATED',NULL,575);
/*!40000 ALTER TABLE `task_log_record` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `task_type`
--

DROP TABLE IF EXISTS `task_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `task_type` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `description` varchar(255) DEFAULT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `task_type`
--

LOCK TABLES `task_type` WRITE;
/*!40000 ALTER TABLE `task_type` DISABLE KEYS */;
INSERT INTO `task_type` VALUES (2,'Tasks of this type are assigned to users that need to sign, or resign an AUP.','SignAUPTask'),(12,'Tasks of this type are assigned to VO admins that need to approve users\' requests.','ApproveUserRequestTask');
/*!40000 ALTER TABLE `task_type` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `user_request_task`
--

DROP TABLE IF EXISTS `user_request_task`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_request_task` (
  `task_id` bigint(20) NOT NULL,
  `req_id` bigint(20) NOT NULL,
  PRIMARY KEY (`task_id`),
  KEY `FKACB7D2932B8C70C` (`task_id`),
  KEY `FKACB7D299D73AC35` (`req_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `user_request_task`
--

LOCK TABLES `user_request_task` WRITE;
/*!40000 ALTER TABLE `user_request_task` DISABLE KEYS */;
/*!40000 ALTER TABLE `user_request_task` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `usr`
--

DROP TABLE IF EXISTS `usr`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `usr` (
  `userid` bigint(20) NOT NULL AUTO_INCREMENT,
  `address` varchar(255) DEFAULT NULL,
  `creation_time` datetime NOT NULL,
  `dn` varchar(255) DEFAULT NULL,
  `email_address` varchar(255) NOT NULL,
  `end_time` datetime NOT NULL,
  `institution` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `phone_number` varchar(255) DEFAULT NULL,
  `surname` varchar(255) DEFAULT NULL,
  `suspended` bit(1) DEFAULT NULL,
  `suspension_reason` varchar(255) DEFAULT NULL,
  `suspension_reason_code` varchar(255) DEFAULT NULL,
  `ca` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`userid`),
  KEY `FK1C5947C6FEB32` (`ca`)
) ENGINE=InnoDB AUTO_INCREMENT=532 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `usr`
--

LOCK TABLES `usr` WRITE;
/*!40000 ALTER TABLE `usr` DISABLE KEYS */;
INSERT INTO `usr` VALUES (21,'MS 127\nP.O. Box 500\nBatavia IL 60510','2012-01-30 13:00:15','/DC=org/DC=doegrids/OU=People/CN=Stephen Kent 93092','skent@fnal.gov','2018-06-01 00:00:00','Fermilab','Stephen','1-630-840-8264','Kent','\0',NULL,'FAILED_TO_SIGN_AUP',NULL),(51,'PIC, Edifici D\nCampus UAB, Bellaterra\n08193 Cerdanyola del Valles\nSpain','2012-01-30 13:00:16','/DC=es/DC=irisgrid/O=pic/CN=christian.neissner','neissner@pic.es','2018-06-01 00:00:00','Port d\'Informacio Cientifica','Christian','0034 935813773','Neissner','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(91,NULL,'2012-01-30 13:00:18','/DC=org/DC=doegrids/OU=People/CN=Steven Timm 74183','timm@fnal.gov','2018-06-01 00:00:00',NULL,NULL,NULL,NULL,'\0',NULL,'FAILED_TO_SIGN_AUP',NULL),(111,'','2012-01-30 13:00:18','/DC=org/DC=doegrids/OU=People/CN=Gabriele Garzoglio 762243','garzogli@fnal.gov','2018-06-01 00:00:00','','Gabriele','','Garzoglio','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(121,NULL,'2012-01-30 13:00:19','/DC=org/DC=doegrids/OU=People/CN=Marko Slyz 664315','mslyz@fnal.gov','2018-06-01 00:00:00',NULL,NULL,NULL,NULL,'','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(142,'1205 West Clark Street\r\nUrbana, iL 61801','2012-08-10 17:13:19','/DC=org/DC=doegrids/OU=People/CN=Chad Kerner 543877','ckerner@fnal.gov','2018-06-01 00:00:00','NCSA','Chad','2172445606','Kerner','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(152,'1205 W. Clark St.\r\n3122B NCSA\r\nUrbana, IL 61801','2012-10-02 16:29:36','/C=US/O=National Center for Supercomputing Applications/OU=People/CN=Weddie Jackson','weddie@ncsa.illinois.edu','2018-06-01 00:00:00','NCSA','Weddie','217-244-5359','Jackson','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(161,'2024 N Racine Ave, Chicago, 60614','2012-10-18 16:41:48','/DC=org/DC=doegrids/OU=People/CN=Tanya Levshina 508821','tlevshin@fnal.gov','2018-06-01 00:00:00','Fermilab','Tanya','6308408730','Levshina','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(171,'WH7W\r\nFermilab\r\nKirk and Pine\r\nBatavia,IL 60510','2012-11-07 13:58:43','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Brian P. Yanny/CN=UID:yanny','yanny@fnal.gov','2018-06-01 00:00:00','Fermilab','Brian','630-840-4413','Yanny','\0',NULL,'FAILED_TO_SIGN_AUP',NULL),(182,'1205 W Clark St,\r\nRoom 2050 A\r\nUrbana, IL 61801','2013-04-10 12:41:09','/C=US/O=National Center for Supercomputing Applications/OU=People/CN=Ankit Chandra','ankitc@ncsa.illinois.edu','2018-06-01 00:00:00','National Center for Supercomputing Applications, UIUC','Ankit','2173334765','Chandra','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(192,'Kirk Rd Pine Street','2013-07-22 09:50:34','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Joe B. Boyd/CN=UID:boyd','boyd@fnal.gov','2018-06-01 00:00:00','Fermilab','Joe','6308408275','Boyd','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(202,'Fermilab, MS 120\r\nP.O. Box 500\r\nBatavia, IL 60510-5011\r\n','2013-09-03 16:39:54','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Marko Slyz 700','mslyz@fnal.gov','2018-06-01 00:00:00','Fermilab','Marko','630 840 6507','Slyz','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(211,'Fermilab, PO Box 500, Batavia IL 60510','2013-09-05 12:11:31','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Marcelle Soares-Santos 1836','marcelle@fnal.gov','2018-06-01 00:00:00','Fermilab','Marcelle','16308408337','Soares-Santos','\0',NULL,NULL,NULL),(212,'NCSA\r\n1205 W. Clark St.\r\nRoom 1008\r\nUrbana, IL 61801','2013-09-26 16:36:06','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Greg Daues 1912','daues@ncsa.uiuc.edu','2018-06-01 00:00:00','NCSA','Greg','317-452-2936','Daues','\0',NULL,NULL,NULL),(221,'1205 W. Clark St.\r\nRoom 1008\r\nUrbana, IL 61801','2013-09-27 12:34:05','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Michelle Gower 1913','mgower@illinois.edu','2018-06-01 00:00:00','NCSA','Michelle','217-244-0387','Gower','\0',NULL,NULL,NULL),(231,'Batavia, IL 60510\nUSA','2013-09-27 12:43:44','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Karl Drlica-Wagner 1914','kadrlica@fnal.gov','2018-06-01 00:00:00','Fermilab','Alex','(630) 840-3662','Drlica-Wagner','\0',NULL,'FAILED_TO_SIGN_AUP',NULL),(241,'Kirk Rd Pine Street\r\nBatavia IL','2014-01-31 10:16:42','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Gerard Bernabeu altayo/CN=UID:gerard1','gerard1@fnal.gov','2018-06-01 00:00:00','Fermilab','Gerard','6308406509','Altayo','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(262,'1205 W. Clark St.\r\nRoom 2050C\r\nUrbana, IL 61801','2014-02-03 10:36:04','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Michael Johnson 2274','mjohns44@illinois.edu','2018-06-01 00:00:00','National Center for Supercomputing Applications','Michael','217-300-0193','Johnson','\0',NULL,NULL,NULL),(272,'MS 369\r\nPO Box 500\r\nBatavia, IL 60510\r\nUSA','2014-02-12 15:03:14','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Bodhitha Jayatilaka 2312','boj@fnal.gov','2018-06-01 00:00:00','Fermilab','Bodhitha','634-840-5110','Jayatilaka','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(282,'MS 369\r\nPO Box 500\r\nBatavia, IL 60510','2014-02-13 13:09:10','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Bodhitha Jayatilaka/CN=UID:boj','boj@fnal.gov','2018-06-01 00:00:00','Fermilab','Bodhitha','630-840-5110','Jayatilaka','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(291,NULL,'2014-04-16 10:31:40','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Nicholas Peregonow/CN=UID:njp','njp@fnal.gov','2018-06-01 00:00:00',NULL,NULL,NULL,NULL,'','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(292,'1205 W Clark st\r\nUrbana, IL 61801\r\n','2014-06-24 11:21:32','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Ricardo Covarrubias 2589','riccov@illinois.edu','2018-06-01 00:00:00','NCSA','Ricardo','2062457190','Covarrubias','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(301,'610 Unquowa Road \r\nFairfield, CT 06824','2014-08-04 09:10:35','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Kathleen Grabowski/CN=UID:kgrabow','kgrabow@fnal.gov','2018-06-01 00:00:00','Fermi National Accelerator Laboratory','Kathleen','1 (203 ) 450-1403','Grabowski','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(302,'Kirk Rd Pine Street','2015-01-09 14:19:31','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Nikolay P. Kuropatkin/CN=UID:kuropat','kuropat@fnal.gov','2018-06-01 00:00:00','Fermilab','Nikolay','6308402416','Kuropatkin','\0','','FAILED_TO_SIGN_AUP',NULL),(312,'kirk rd pine st','2015-03-04 16:56:09','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Dennis D. Box/CN=UID:dbox','dbox@fnal.gov','2018-06-01 00:00:00','Fermilab','Dennis','6308403000','Box','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(322,'National Center for Supercomputing Applications\r\n1205 W Clark St, \r\nUrbana, IL 61801\r\n','2015-04-07 13:10:57','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Felipe Menanteau 3137','felipe@illinois.edu','2018-06-01 00:00:00','NCSA, University of Illinois','Felipe','217-244-6297','Menanteau','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(331,'MS 369\r\nPO Box 500\r\nBatavia, IL 60510','2015-05-05 11:38:30','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Kenneth R. Herner/CN=UID:kherner','kherner@fnal.gov','2018-06-01 00:00:00','Fermilab','Kenneth','6308406783','Herner','\0',NULL,NULL,NULL),(341,'209 South 33rd Street\r\nPhiladelphia, PA 19104','2015-05-19 14:09:45','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Masao Sako 3215','masao@sas.upenn.edu','2018-06-01 00:00:00','University of Pennsylvania','Masao','1-215-898-8151','Sako','\0',NULL,'FAILED_TO_SIGN_AUP',NULL),(351,'Jodrell Bank Centre for Astrophysics\r\nThe University of Manchester\r\nAlan Turing Building\r\nManchester\r\nM13 9PL\r\nUnited Kingdom\r\n','2015-06-01 11:47:30','/C=UK/O=eScience/OU=Manchester/L=HEP/CN=joe zuntz','joseph.zuntz@manchester.ac.uk','2018-06-01 00:00:00','University of Manchester','Joe','07746367297','Zuntz','\0',NULL,'FAILED_TO_SIGN_AUP',NULL),(361,'Fermilab WH 7W','2015-06-05 11:56:19','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Flavia Sobreira sanchez/CN=UID:sobreira','sobreira@fnal.gov','2018-06-01 00:00:00','Fermilab','Flavia','x4413','Sobreira Sanchez','\0',NULL,NULL,NULL),(371,'Fermilab WH7W','2015-06-09 14:45:02','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Huan Lin/CN=UID:hlin','hlin@fnal.gov','2018-06-01 00:00:00','Fermilab','Huan ','x8452','Lin','\0',NULL,NULL,NULL),(381,'SiDet Lab Fermilab','2015-06-09 14:52:12','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Lalith P. Perera/CN=UID:perera','perera@fnal.gov','2018-06-01 00:00:00','Fermilab','Lalith','x2441','Perera','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(382,'1205 W Clark St\r\nUrbana, IL 61801-2311','2015-07-07 10:40:13','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Eric Morganson 3317','ericm@illinois.edu','2018-06-01 00:00:00','NCSA','Eric','6032896476','Morganson','\0',NULL,NULL,NULL),(392,NULL,'2015-07-24 15:38:51','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Thomas R. Junk/CN=UID:trj','trj@fnal.gov','2018-06-01 00:00:00',NULL,NULL,NULL,NULL,'','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(401,'1205 W. Clark St., MC-257\r\nRoom 2050A\r\nUrbana, IL 61801','2015-07-29 15:25:59','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Douglas Nathaniel Friedel 3371','friedel@illinois.edu','2018-06-01 00:00:00','National Center for Supercomputing Applications','Douglas','217-333-9378','Friedel','\0',NULL,NULL,NULL),(403,'kirk rd pine st','2015-09-14 10:49:05','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Dmitry O. Litvintsev/CN=UID:litvinse','litvintse@fnal.gov','2018-06-01 00:00:00','fermilab','Dmitry','6308406791','Litvintse','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(406,'Fermilab\r\nMS 127\r\nPO Box 500\r\nBatavia, IL 60510','2015-09-18 10:44:51','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Douglas L. Tucker/CN=UID:dtucker','dtucker@fnal.gov','2018-06-01 00:00:00','Fermilab','Douglas','630-840-2267','Tucker','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(409,'FNAL','2015-09-18 17:09:57','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Sahar Allam/CN=UID:sallam','sallam@fnal.gov','2018-06-01 00:00:00','FNAL','Sahar','6308406506','Allam','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(411,'Mail Station 127\r\nFermi National Accelerator Laboratory\r\nP. O. Box 500\r\nBatavia, IL 60510-0500','2015-10-12 10:15:25','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Eric H. NeilsenJr./CN=UID:neilsen','neilsen@fnal.gov','2018-06-01 00:00:00','Fermilab','Eric','630 840 6720','Neilsen','\0',NULL,NULL,NULL),(412,'908 S. Webber St.\r\nUrbana IL 61801','2015-11-22 12:52:52','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Greg Daues 1912','daues@ncsa.uiuc.edu','2018-06-01 00:00:00','NCSA','Greg','317-452-2936','Daues','\0',NULL,NULL,NULL),(415,'PO Box 500\r\nBatavia, IL 60510','2015-12-22 14:11:16','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=James Annis/CN=UID:annis','annis@fnal.gov','2018-06-01 00:00:00','Fermilab','James','(630) 840-5181','Annis','\0',NULL,NULL,NULL),(417,'133 Astronomy\r\n1002 W. Green St.\r\nUrbana, IL\r\nMC 221','2016-01-29 13:58:25','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Xinyang Lu 3630','xlu28@illinois.edu','2018-06-01 00:00:00','University of Illinois at Urbana Champaign','Xinyang','2179790468','Lu','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(423,'SLAC\r\nStanford University \r\nPalo Alto, CA','2016-03-15 14:17:47','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Mandeep Gill/CN=UID:mssgill','msgill@slac.stanford.edu','2018-06-01 00:00:00','SLAC,Standford University','Mandeep','630-840-4413','Gill','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(427,'1205 W. Clark St., Urbana, IL 61801','2016-03-28 11:01:36','/DC=com/DC=DigiCert-Grid/O=Open Science Grid/OU=People/CN=Michael Johnson 2274','mjohns44@illinois.edu','2018-06-01 00:00:00','National Center for Supercomputing Applications','Michael','217-300-0193','Johnson','\0',NULL,NULL,NULL),(433,'unknown','2016-04-14 15:12:00','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Thomas Diehl/CN=UID:diehl','diehl@fnal.gov','2018-06-01 00:00:00','Fermilab','Thomas',' unknown','Diehl','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(439,'unknown','2016-05-13 02:13:12','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Stephanie J. Hamilton/CN=UID:hamil332','sjhamil@umich.edu','2018-06-01 00:00:00','University of Michigan','Stephanie',' unknown','Hamilton','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(442,'unknown','2016-05-13 02:27:47','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Ting Li/CN=UID:tingli','tingli@fnal.gov','2018-06-01 00:00:00','TAMU','Ting','unknown','Li','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(445,'unknown','2016-05-13 02:39:17','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Manuel G. Fernandez/CN=UID:mgarcia','unknown@fnal.gov','2018-06-01 00:00:00','CIEMAT','Manuel','unknown','Garcia Fernandez','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(448,'unknown','2016-05-13 02:43:22','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Youngsoo Park/CN=UID:yspark1','yougsoo@email.arizona.edu','2018-06-01 00:00:00','University of Arizona','Youngsoo','unknown','Park','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(451,'unknown','2016-05-13 02:49:03','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Luiz alberto N. Da costa/CN=UID:ldacosta','ldacosta@linea.gov.br','2018-06-01 00:00:00','LINEA','Luiz','unknown','Dacosta','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(454,'unknown','2016-05-13 02:56:34','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Yuanyuan Zhang/CN=UID:ynzhang','ynzhang@fnal.gov','2018-06-01 00:00:00','Fermilab','Yuanyuan','unknown','Zhang','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(457,'unknown','2016-05-13 11:53:56','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Michel Aguena da silva/CN=UID:aguena','aguena@if.usp.br','2018-06-01 00:00:00','unknown','Michel','unknown','Aguena','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(460,'unknown','2016-05-13 12:34:06','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Ignacio N. Sevilla/CN=UID:nsevilla','nsevilla@fnal.gov','2018-06-01 00:00:00','unknown','Ignacio','unknown','Sevilla','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(463,'unknown','2016-05-13 12:38:16','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Angelo Fausti neto/CN=UID:fausti','angelofausti@gmail.com','2018-06-01 00:00:00','unknown','Angelo','unknown','Neto','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(466,'unknown','2016-05-13 12:40:47','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Riccardo Campisano/CN=UID:rcampisa','riccardo.campisano@linea.gov.br','2018-06-01 00:00:00','unknown','Riccardo','unknown','Campisano','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(469,'unknown','2016-05-13 12:44:29','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Carlos A. Souza/CN=UID:adean','carlosadean@gmail.com','2018-06-01 00:00:00','unknown','Carlos','unknown','Adean','','User failed to sign the AUP in time.','FAILED_TO_SIGN_AUP',NULL),(475,'unknown','2016-05-25 13:26:54','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Hallie F. Gaitsch/CN=UID:hgaitsch','hgaitsch@fnal.gov','2018-06-01 00:00:00','unknown','Hallie','unknown','Gaitsch','\0',NULL,NULL,NULL),(481,'unknown','2016-06-07 11:25:17','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Dillon Brout/CN=UID:djbrout','dbrout@physics.upenn.edu','2018-06-01 00:00:00','University of Pennsylvania','Dillon','unknown','Brout','\0',NULL,NULL,NULL),(487,'PO. box 500 batavia, IL ','2016-06-13 16:41:07','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Lisa Giacchetti/CN=UID:lisa','lisa@fnal.gov','2018-06-01 00:00:00','Fermilab','Lisa','630-840-8023','Giacchetti','\0',NULL,NULL,NULL),(490,'Wilson Street and Kirk Road, Batavia, IL 60510','2016-06-30 16:58:28','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Amanda Gao/CN=UID:agao','agao@fnal.gov','2018-06-01 00:00:00','Fermilab ','Amanda ','(630) 840-3351','Gao','\0',NULL,NULL,NULL),(496,'unknown','2016-07-06 17:27:19','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Timothy Osborn/CN=UID:tosborn','tosborn@fnal.gov','2018-06-01 00:00:00','unknown','Timothy','unknown','Osborn','\0',NULL,NULL,NULL),(499,'unknown','2016-07-12 16:44:05','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Paul Chichura/CN=UID:pchich','pchich@sas.upenn.edu','2018-06-01 00:00:00','University of Pennsylvania','Paul','unknown','Chichura','\0',NULL,NULL,NULL),(501,'unknown','2016-09-07 11:16:50','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Christopher Annis/CN=UID:cannis','cannis@fnal.gov','2018-06-01 00:00:00','unknown','Christopher','unknown','Annis','\0',NULL,NULL,NULL),(503,'unknown','2016-09-21 19:31:14','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Antonella Palmese/CN=UID:palmese','antonella.palmese.13@ucl.ac.uk','2018-06-01 00:00:00','University College London','Antonella','unknown','Palmese','\0',NULL,NULL,NULL),(509,'unknown','2016-11-04 13:33:14','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Robert Butler/CN=UID:rbutler','rbutler@fnal.gov','2018-06-01 00:00:00','University of Chicago','Robert','unknown','Butler','\0',NULL,NULL,NULL),(511,'National Center for Supercomputing Applications\r\nUniversity of Illinois at Urbana-Champaign\r\n1205 W. Clark St., MC-257\r\nRoom 2050 F\r\nUrbana, Illinois\r\n61801','2016-12-02 10:26:36','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Francisco Paz-Chinchon 4084','fpazch@illinois.edu','2018-06-01 00:00:00','NCSA, University of Illinois','Francisco','217 300 4689','Paz-Chinchon','\0',NULL,NULL,NULL),(513,'1010 W Main St Apt 209','2016-12-06 13:40:30','/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=People/CN=Yu-Ching Chen 4082','ycchen@illinois.edu','2018-06-01 00:00:00','NCSA','Yu-Ching','224-249-1144','Chen','\0',NULL,NULL,NULL),(519,'Fermilab\r\nKirk Rd. at Pine St.\r\nBatavia, IL 60510','2017-01-04 15:17:11','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Michael Wang/CN=UID:mwang','mwang@fnal.gov','2018-06-01 00:00:00','Fermilab','Michael','(630)840-2947','Wang','\0',NULL,NULL,NULL),(525,'Fermilab\r\nPO Box 500','2017-01-26 13:57:54','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Scott Dodelson/CN=UID:dodelson','dodelson@fnal.gov','2018-06-01 00:00:00','Fermilab','Scott','6308402426','Dodelson','\0',NULL,NULL,NULL),(531,'unknown','2017-02-08 12:59:54','/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=Zoheyr Doctor/CN=UID:zdoctor','zdoctor@uchicago.edu','2018-06-01 00:00:00','University of Chicago','Zoheyr','unknown','Doctor','\0',NULL,NULL,NULL);
/*!40000 ALTER TABLE `usr` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `usr_attrs`
--

DROP TABLE IF EXISTS `usr_attrs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `usr_attrs` (
  `a_id` bigint(20) NOT NULL,
  `u_id` bigint(20) NOT NULL,
  `a_value` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`a_id`,`u_id`),
  KEY `FKA39E0E37566C2A8F` (`a_id`),
  KEY `FKA39E0E3720331206` (`u_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `usr_attrs`
--

LOCK TABLES `usr_attrs` WRITE;
/*!40000 ALTER TABLE `usr_attrs` DISABLE KEYS */;
/*!40000 ALTER TABLE `usr_attrs` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `version`
--

DROP TABLE IF EXISTS `version`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `version` (
  `version` int(11) NOT NULL,
  `admin_version` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`version`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `version`
--

LOCK TABLES `version` WRITE;
/*!40000 ALTER TABLE `version` DISABLE KEYS */;
INSERT INTO `version` VALUES (3,'2.6.1');
/*!40000 ALTER TABLE `version` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vo_membership_req`
--

DROP TABLE IF EXISTS `vo_membership_req`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vo_membership_req` (
  `confirmId` varchar(255) NOT NULL,
  `request_id` bigint(20) NOT NULL,
  PRIMARY KEY (`request_id`),
  KEY `FK28EE8AFBD75D60A4` (`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vo_membership_req`
--

LOCK TABLES `vo_membership_req` WRITE;
/*!40000 ALTER TABLE `vo_membership_req` DISABLE KEYS */;
INSERT INTO `vo_membership_req` VALUES ('c680fbe6-66bf-4581-9d01-75035e728f70',219),('30e6c219-5fd2-4ce3-b7bf-352f154440d2',225);
/*!40000 ALTER TABLE `vo_membership_req` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2017-07-06 14:28:25
