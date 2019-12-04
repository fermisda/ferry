

CREATE TABLE user_deletions (
	uid                  bigint  NOT NULL ,
	uname                varchar(100)  NOT NULL ,
	status               bool DEFAULT false NOT NULL ,
	expiration_date      date   ,
	last_updated         timestamptz DEFAULT ('now'::text)::date NOT NULL ,
	full_name            varchar(255)   ,
	is_groupaccount      bool DEFAULT false NOT NULL ,
	when_deleted         timestamptz  NOT NULL
 ) ;

COMMENT ON COLUMN user_deletions.uid IS 'unix user id';

COMMENT ON COLUMN user_deletions.uname IS 'user unix name';

CREATE TABLE user_group_deletions (
	uid                  bigint  NOT NULL ,
	groupid              integer  NOT NULL ,
	is_leader            bool DEFAULT false NOT NULL ,
	last_updated         timestamptz DEFAULT ('now'::text)::date NOT NULL,
	when_deleted         timestamptz  NOT NULL
 ) ;


\i grants.sql
