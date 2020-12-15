
ALTER TABLE "public".affiliation_unit_group ADD is_wilsoncluster boolean DEFAULT false NOT NULL ;

CREATE UNIQUE INDEX unq_affiliation_unit_group_unitid_is_wilsoncluster ON "public".affiliation_unit_group ( unitid, is_wilsoncluster )
  where is_wilsoncluster IS TRUE;
