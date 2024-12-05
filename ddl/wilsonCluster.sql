-- Tables with no wilsoncluster data:
--  storage_quota
--  grid_fqan
--  compute_access_group

-- for dev effort
-- drop table if exists wc_compute_access_group;
-- drop table if exists wc_compute_access;
-- drop table if exists wc_compute_batch;
-- drop table if exists wc_compute_resources;
-- drop table if exists wc_user_group;
-- drop table if exists wc_affiliation_unit_group;
-- drop table if exists wc_groups;

-- First copy the WilsonCuster data to backup tables with the prefixed with wc_ .

create table wc_compute_access_group as
   (select * from compute_access_group where compid in
   (select compid from compute_resources where name = 'wilson_cluster'));

create table wc_compute_access as
   (select * from compute_access where compid in
   (select compid from compute_resources where name = 'wilson_cluster'));

create table wc_compute_batch as
   (select * from compute_batch where compid in
   (select compid from compute_resources where name = 'wilson_cluster'));

create table wc_compute_resources as
    (select * from compute_resources where name = 'wilson_cluster');

create table wc_user_group as
    (select * from user_group where groupid in
    (select groupid from groups where type = 'WilsonCluster'));

create table wc_affiliation_unit_group as
    (select * from affiliation_unit_group  where groupid in
    (select groupid from groups where type = 'WilsonCluster'));

create table wc_groups as
    (select * from groups where type = 'WilsonCluster');

-- Now delete the WilsonCluster data from the original tables.

begin;

delete from compute_access_group where compid in
   (select compid from compute_resources where name = 'wilson_cluster');

delete from  compute_access where compid in
    (select compid from compute_resources where name = 'wilson_cluster');

delete from compute_batch where compid in
    (select compid from compute_resources where name = 'wilson_cluster');

delete from  compute_resources where name = 'wilson_cluster';

delete from  user_group where groupid in
    (select groupid from groups where type = 'WilsonCluster');

delete from  affiliation_unit_group where groupid in
    (select groupid from groups where type = 'WilsonCluster');

delete from  groups where type = 'WilsonCluster';

-- need to do commit or rollback
