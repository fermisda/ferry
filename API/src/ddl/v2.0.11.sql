
drop table nas_storage;

ALTER TABLE "public".users ADD is_sharedaccount boolean DEFAULT false NOT NULL;

CREATE  TABLE "public".compute_resource_shared_account (
	sharedaccount_uid    bigint  NOT NULL ,
	uid                  bigint  NOT NULL ,
	compid               bigint  NOT NULL ,
	is_leader            boolean DEFAULT false NOT NULL ,
	last_updated         timestamptz DEFAULT ('now'::text)::date NOT NULL,
	CONSTRAINT pk_compute_resource_shared_account_sharedaccount_uid PRIMARY KEY ( sharedaccount_uid, uid, compid )
 ) ;

CREATE UNIQUE INDEX idx_compute_resource_shared_account ON "public".compute_resource_shared_account ( uid, sharedaccount_uid, compid ) ;

ALTER TABLE "public".compute_resource_shared_account ADD CONSTRAINT fk_compute_resource_shared_account_compute_resources FOREIGN KEY ( compid ) REFERENCES "public".compute_resources( compid )   ;

ALTER TABLE "public".compute_resource_shared_account ADD CONSTRAINT fk_compute_resource_shared_account_users_0 FOREIGN KEY ( uid ) REFERENCES "public".users( uid )   ;

ALTER TABLE "public".compute_resource_shared_account ADD CONSTRAINT fk_compute_resource_shared_account_users FOREIGN KEY ( sharedaccount_uid ) REFERENCES "public".users( uid )   ;



grant select, insert, update, delete on all tables in schema public to ferry_writer;

-- Function and associated triggers to ensure last_updated is changed even when inserted/updated through psql.

CREATE OR REPLACE FUNCTION common_update_stamp() RETURNS trigger AS $common_update_stamp$
    BEGIN
        IF TG_OP='UPDATE' then
            IF NEW.last_updated IS NULL OR NEW.last_updated = OLD.last_updated THEN
                NEW.last_updated := current_timestamp;
            END IF;
        ELSE
            IF NEW.last_updated IS NULL THEN
                NEW.last_updated := current_timestamp;
            END IF;
        END IF;
        RETURN NEW;
    END;
$common_update_stamp$ LANGUAGE plpgsql;

CREATE TRIGGER user_group_common_update_stamp BEFORE INSERT OR UPDATE ON user_group
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER users_common_update_stamp BEFORE INSERT OR UPDATE ON users
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER external_affiliation_attribute_common_update_stamp BEFORE INSERT OR UPDATE ON external_affiliation_attribute
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER user_certificates_common_update_stamp BEFORE INSERT OR UPDATE ON user_certificates
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER compute_access_group_common_update_stamp BEFORE INSERT OR UPDATE ON compute_access_group
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER compute_access_common_update_stamp BEFORE INSERT OR UPDATE ON compute_access
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER compute_resources_common_update_stamp BEFORE INSERT OR UPDATE ON compute_resources
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER compute_batch_common_update_stamp BEFORE INSERT OR UPDATE ON compute_batch
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER groups_common_update_stamp BEFORE INSERT OR UPDATE ON groups
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER storage_quota_common_update_stamp BEFORE INSERT OR UPDATE ON storage_quota
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER storage_resources_common_update_stamp BEFORE INSERT OR UPDATE ON storage_resources
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER grid_access_common_update_stamp BEFORE INSERT OR UPDATE ON grid_access
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER grid_fqan_common_update_stamp BEFORE INSERT OR UPDATE ON grid_fqan
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER voms_url_common_update_stamp BEFORE INSERT OR UPDATE ON voms_url
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER affiliation_units_common_update_stamp BEFORE INSERT OR UPDATE ON affiliation_units
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER affiliation_unit_user_certificate_common_update_stamp BEFORE INSERT OR UPDATE ON affiliation_unit_user_certificate
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER affiliation_unit_common_update_stamp BEFORE INSERT OR UPDATE ON affiliation_unit_group
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();

CREATE TRIGGER compute_resource_shared_account_common_update_stamp BEFORE INSERT OR UPDATE ON compute_resource_shared_account
    FOR EACH ROW EXECUTE PROCEDURE common_update_stamp();
