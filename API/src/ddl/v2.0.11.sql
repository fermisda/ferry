
ALTER TABLE "public".groups ADD external_attributes json   ;

-- Function and associated triggers to ensure last_updated is changed even when inserted/updated through psql.

CREATE OR REPLACE FUNCTION common_update_stamp() RETURNS trigger AS $common_update_stamp$
    BEGIN
        IF NEW.last_updated IS NULL OR NEW.last_updated = OLD.last_updated THEN
            NEW.last_updated := current_timestamp;
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

CREATE TRIGGER nas_storage_common_update_stamp BEFORE INSERT OR UPDATE ON nas_storage
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
