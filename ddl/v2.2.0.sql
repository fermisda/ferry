

ALTER TABLE "public".accessors DROP CONSTRAINT check_type;

ALTER TABLE "public".accessors ADD CONSTRAINT check_type CHECK ( (type = ANY (ARRAY[('dn_role'::character varying)::text,
    ('ip_role'::character varying)::text, ('jwt_role'::character varying)::text])) );

-- INSERTS a jwt_role record in accessors for each DN with a matching entry in the users table.
WITH jwt_users AS (
    select split_part(name, ':', 2) as uname, active,write
    from accessors where name like '%:%'
), jwt_insert AS (
    select voPersonId, jwt_users.active, jwt_users.write, jwt_users.uname
    from users join jwt_users using (uname)
)
insert into accessors (name, active, write, type, comments)
select voPersonid, active, write, 'jwt_role', uname
from jwt_insert
;
