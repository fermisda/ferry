-- Move the glexecXXXX groups off to a temp table.
-- We can delete the table after this has sat for a while.
-- Those groups were requested by Steve Timm and are no
-- longer needed.

create table groups_glexec as
  select * from groups
  where name like 'glexec%'
    and name != 'glexec';

begin;
  delete from groups
  where name like 'glexec%'
    and name != 'glexec';
  select * from groups
  where name like 'glexec%'
    and name != 'glexec';
  select * from groups where name = 'glexec';
commit;
