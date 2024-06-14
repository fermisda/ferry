
CREATE  TABLE "public".user_affiliation_units (
	uid                  bigint  NOT NULL  ,
	unitid               integer  NOT NULL  ,
	last_updated         timestamptz DEFAULT ('now'::text)::date NOT NULL  ,
	CONSTRAINT pk_user_affiliation_units PRIMARY KEY ( uid, unitid )
 );

ALTER TABLE "public".user_affiliation_units ADD CONSTRAINT fk_user_affiliation_units_users FOREIGN KEY ( uid )
  REFERENCES "public".users( uid );

ALTER TABLE "public".user_affiliation_units ADD CONSTRAINT fk_user_affiliation_units_affiliation_units FOREIGN KEY ( unitid )
  REFERENCES "public".affiliation_units( unitid );

CREATE TRIGGER user_affiliation_units_common_update_stamp BEFORE INSERT OR UPDATE ON public.user_affiliation_units
  FOR EACH ROW EXECUTE FUNCTION common_update_stamp()

\i grants.sql
