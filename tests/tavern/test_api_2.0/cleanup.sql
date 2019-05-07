delete from grid_access where uid=1136 and fqanid=52;
delete from compute_access_group where compid=74 and uid=6956;
delete from compute_access where compid=74 and uid=6956;
select removeUserFromExperiment('napier', 'ebd');
delete from user_group where groupid=5485 and uid=1136;
