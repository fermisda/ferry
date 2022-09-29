
DROP TABLE "public".user_deletions CASCADE;

DROP TABLE "public".user_group_deletions CASCADE;

ALTER TABLE "public".users ADD in_ldap boolean DEFAULT false NOT NULL  ;

update users set in_ldap=true where vopersonid is not null ;
