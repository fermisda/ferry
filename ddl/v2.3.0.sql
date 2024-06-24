
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

ALTER TABLE users RENAME COLUMN vopersonid TO token_subject;

\i grants.sql

-- Determine the exps each person is in from affiliation_unit_user_certificate and set it in the new table.
insert into user_affiliation_units (uid, unitid)
  (select distinct u.uid, au.unitid
   from user_certificates as uc
     join affiliation_unit_user_certificate as auuc using (dnid)
     join affiliation_units as au using (unitid)
     join users as u using (uid)
   where dn like '/DC=org/DC=cilogon/C=US/O=Fermi National Accelerator Laboratory/OU=People/CN=%/CN=UID:%')
;
