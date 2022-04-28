


ALTER TABLE users ADD vopersonid uuid;
ALTER TABLE users ADD CONSTRAINT unq_users UNIQUE ( vopersonid );

ALTER TABLE user_deletions ADD vopersonid uuid;


-- Now run syncLdapWithFerry which will update all the LDAP and Ferry records.
-- Then run this delete command

-- delete from external_affiliation_attribute where attribute = 'voPersonID';
