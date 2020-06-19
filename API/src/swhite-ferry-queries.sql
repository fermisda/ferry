
select u.uname, ug.uid, ug.groupid, g.name as groupname, au.name as affiliationname
from users as u
left join user_group as ug using(uid)
left join groups as g using(groupid)
left join affiliation_unit_group aug using (groupid)
left join affiliation_units au using (unitid)
where uname = 'swhite'
order by au.name;

select g.name as "group name", au.name as "affil name", g.groupid, aug.unitid, aug.is_primary, aug.is_required
from groups as g
left join affiliation_unit_group as aug using(groupid)
left join affiliation_units as au using(unitid)
where au.name ='dune';

delete  from compute_access_group where uid=1627 and groupid=1424;
delete from user_group where uid=1627 and groupid=1424;
delete from user_group where uid=1627 and groupid=2373;


select ga.fqanid, gf.fqan, gf.mapped_user, gf.mapped_group
from grid_access as ga
join grid_fqan as gf using(fqanid);
where ga.uid = 1627;


select name, fqan, uname, full_name
from grid_access
  join grid_fqan using(fqanid)
  join users using(uid)
  left join affiliation_units using(unitid)
where uname = 'swhite'



****


select name from groups where groupid in (
  select groupid from groups
  except
    select distinct groupid from
      (select distinct groupid from user_group
      union all
      select distinct mapped_group as groupid from grid_fqan
      union all
      select distinct groupid from storage_quota
      union all
      select distinct groupid from affiliation_unit_group
      ) as all_used
)
order by name


*****


select * from groups where name='icarus';
select * from affiliation_unit_group where unitid = (select unitid from affiliation_units where name='icarus');

select * from groups where type = 'WilsonCluster';
select * from affiliation_unit_group where groupid in (select groupid from groups where type = 'WilsonCluster');


select * from users where uname = 'dkplant';
select * from compute_access_group where uid = 4321 and compid = 79;
select * from compute_access where uid = 4321;
