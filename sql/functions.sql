CREATE OR REPLACE FUNCTION removeUserFromExperiment(u text, e text) RETURNS bool LANGUAGE plpgsql AS $$
DECLARE
    uidValue int;
    unitidValue int;
    compidList int[];
    fqanidList int[];
    dnidList int[];
BEGIN
    SELECT uid INTO uidValue FROM users WHERE uname = u;
    SELECT unitid INTO unitidValue FROM affiliation_units WHERE name = e;
    SELECT ARRAY(SELECT compid FROM compute_resources WHERE unitid = unitidValue) INTO compidList;
    SELECT ARRAY(SELECT fqanid FROM grid_fqan WHERE unitid = unitidValue) INTO fqanidList;
    SELECT ARRAY(SELECT dnid FROM user_certificates WHERE uid = uidValue) INTO dnidList;

    DELETE FROM storage_quota WHERE uid = uidValue AND unitid = unitidValue;
    DELETE FROM compute_access_group WHERE uid = uidValue AND compid = ANY(compidList);
    DELETE FROM compute_access WHERE uid = uidValue AND compid = ANY(compidList);
    DELETE FROM grid_access WHERE uid = uidValue AND fqanid = ANY(fqanidList);
    DELETE FROM affiliation_unit_user_certificate WHERE unitid = unitidValue AND dnid = ANY(dnidList);

    IF e = 'cms' THEN
        DELETE FROM external_affiliation_attribute WHERE uid = uidValue AND attribute = 'cern_username';
    END IF;

    RETURN TRUE;
END
$$;