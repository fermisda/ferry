

CREATE TABLE user_deletions (
	uid                  bigint  NOT NULL ,
	uname                varchar(100)  NOT NULL ,
	full_name            varchar(255)   ,
	status               bool DEFAULT false NOT NULL ,
	expiration_date      date   ,
	last_updated         timestamptz DEFAULT ('now'::text)::date NOT NULL ,
	is_groupaccount      bool DEFAULT false NOT NULL ,
	when_deleted         timestamptz  NOT NULL ,
	CONSTRAINT idx_user_deletions PRIMARY KEY ( uid ),
	CONSTRAINT idx_user_deletions_0 UNIQUE ( uname )
 ) ;

COMMENT ON COLUMN user_deletions.uid IS 'unix user id';

COMMENT ON COLUMN user_deletions.uname IS 'user unix name';

CREATE TABLE user_group_deletions (
	uid                  bigint  NOT NULL ,
	groupid              integer  NOT NULL ,
	is_leader            bool DEFAULT false NOT NULL ,
	last_updated         timestamptz DEFAULT ('now'::text)::date NOT NULL ,
	CONSTRAINT pk_user_group_0 PRIMARY KEY ( uid, groupid )
 ) ;

CREATE INDEX idx_user_group_uid_0 ON user_group_deletions ( uid ) ;

CREATE INDEX idx_user_group_groupid_0 ON user_group_deletions ( groupid ) ;

ALTER TABLE user_group_deletions ADD CONSTRAINT fk_user_group_user_deletions FOREIGN KEY ( uid ) REFERENCES user_deletions( uid )  ;

\i grants.sql
