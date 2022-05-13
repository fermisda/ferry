


ALTER TABLE users ADD vopersonid uuid;
ALTER TABLE users ADD CONSTRAINT unq_users UNIQUE ( vopersonid );

ALTER TABLE user_deletions ADD vopersonid uuid;


