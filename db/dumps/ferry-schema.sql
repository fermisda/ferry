--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

--
-- Name: groups_group_type; Type: TYPE; Schema: public; Owner: ferry
--

CREATE TYPE groups_group_type AS ENUM (
    'UnixGroup',
    'PhysicsGroup',
    'OrgChartGroup'
);


ALTER TYPE public.groups_group_type OWNER TO ferry;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: collaboration_unit; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE collaboration_unit (
    unitid bigint NOT NULL,
    unit_name character varying(100) NOT NULL,
    voms_url character varying(200),
    alternative_name character varying(100),
    last_updated date DEFAULT ('now'::text)::date
);


ALTER TABLE public.collaboration_unit OWNER TO ferry;

--
-- Name: TABLE collaboration_unit; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON TABLE collaboration_unit IS 'experiments and projects';


--
-- Name: COLUMN collaboration_unit.unitid; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON COLUMN collaboration_unit.unitid IS 'Fermilab collaboration unit id ';


--
-- Name: COLUMN collaboration_unit.voms_url; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON COLUMN collaboration_unit.voms_url IS 'url to relevant voms installation. could point to a subgroup within fermilab voms';


--
-- Name: collaboration_unit_group; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE collaboration_unit_group (
    unitid bigint NOT NULL,
    groupid bigint NOT NULL,
    is_primary smallint,
    last_updated date DEFAULT ('now'::text)::date
);


ALTER TABLE public.collaboration_unit_group OWNER TO ferry;

--
-- Name: compute_access; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE compute_access (
    compid bigint NOT NULL,
    uid bigint NOT NULL,
    groupid bigint NOT NULL,
    shell character varying(30) DEFAULT '/bin/bash'::character varying NOT NULL,
    last_updated date DEFAULT ('now'::text)::date NOT NULL,
    home_dir character varying(100)
);


ALTER TABLE public.compute_access OWNER TO ferry;

--
-- Name: compute_batch; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE compute_batch (
    compid bigint DEFAULT (0)::bigint NOT NULL,
    groupid bigint NOT NULL,
    name character varying(300) NOT NULL,
    value bigint,
    type character varying(255),
    last_updated date DEFAULT ('now'::text)::date
);


ALTER TABLE public.compute_batch OWNER TO ferry;

--
-- Name: compute_resource; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE compute_resource (
    compid bigint NOT NULL,
    name character varying(100),
    default_shell character varying(100),
    comp_type character varying(100),
    unitid integer,
    last_updated date DEFAULT ('now'::text)::date,
    default_home_dir character varying(100)
);


ALTER TABLE public.compute_resource OWNER TO ferry;

--
-- Name: grid_fqan; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE grid_fqan (
    fqanid bigint NOT NULL,
    fqan character varying(300) NOT NULL,
    mapped_user character varying(100),
    mapped_group character varying(100) NOT NULL,
    last_updated date DEFAULT ('now'::text)::date
);


ALTER TABLE public.grid_fqan OWNER TO ferry;

--
-- Name: experiment_roles_roleid_seq; Type: SEQUENCE; Schema: public; Owner: ferry
--

CREATE SEQUENCE experiment_roles_roleid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.experiment_roles_roleid_seq OWNER TO ferry;

--
-- Name: experiment_roles_roleid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ferry
--

ALTER SEQUENCE experiment_roles_roleid_seq OWNED BY grid_fqan.fqanid;


--
-- Name: experiments_expid_seq; Type: SEQUENCE; Schema: public; Owner: ferry
--

CREATE SEQUENCE experiments_expid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.experiments_expid_seq OWNER TO ferry;

--
-- Name: experiments_expid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ferry
--

ALTER SEQUENCE experiments_expid_seq OWNED BY collaboration_unit.unitid;


--
-- Name: grid_access; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE grid_access (
    uid bigint NOT NULL,
    unitid bigint NOT NULL,
    fqanid bigint NOT NULL,
    is_superuser boolean,
    is_banned boolean,
    last_updated date DEFAULT ('now'::text)::date NOT NULL
);


ALTER TABLE public.grid_access OWNER TO ferry;

--
-- Name: groups; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE groups (
    gid bigint,
    group_name character varying(100) NOT NULL,
    group_type groups_group_type NOT NULL,
    groupid bigint NOT NULL,
    last_updated date DEFAULT ('now'::text)::date
);


ALTER TABLE public.groups OWNER TO ferry;

--
-- Name: TABLE groups; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON TABLE groups IS 'unix group';


--
-- Name: COLUMN groups.gid; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON COLUMN groups.gid IS 'group unix id';


--
-- Name: COLUMN groups.group_name; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON COLUMN groups.group_name IS 'unix group name';


--
-- Name: storage_quota; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE storage_quota (
    groupid bigint,
    path text NOT NULL,
    last_updated date DEFAULT ('now'::text)::date NOT NULL,
    shell character varying(255),
    value text NOT NULL,
    unit character varying(100) NOT NULL,
    valid_until date,
    quotaid bigint NOT NULL,
    storageid bigint NOT NULL,
    uid bigint
);


ALTER TABLE public.storage_quota OWNER TO ferry;

--
-- Name: TABLE storage_quota; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON TABLE storage_quota IS 'table store quota per user in various storages';


--
-- Name: storage_resource; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE storage_resource (
    storageid bigint NOT NULL,
    name character varying(100) NOT NULL,
    storage_type character varying(255) NOT NULL,
    default_path character varying(255),
    default_quota bigint,
    last_updated date DEFAULT ('now'::text)::date
);


ALTER TABLE public.storage_resource OWNER TO ferry;

--
-- Name: user_affiliation; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE user_affiliation (
    uid bigint NOT NULL,
    affiliation_value character varying(100),
    last_updated date DEFAULT ('now'::text)::date,
    affiliation_attribute character varying(100) NOT NULL
);


ALTER TABLE public.user_affiliation OWNER TO ferry;

--
-- Name: user_certificate; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE user_certificate (
    uid bigint NOT NULL,
    dn character varying(300) NOT NULL,
    issuer_ca character varying(120) NOT NULL,
    last_update date DEFAULT ('now'::text)::date NOT NULL,
    unitid bigint NOT NULL
);


ALTER TABLE public.user_certificate OWNER TO ferry;

--
-- Name: user_group; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE user_group (
    uid bigint NOT NULL,
    groupid bigint NOT NULL,
    is_leader boolean,
    last_updated date DEFAULT ('now'::text)::date
);


ALTER TABLE public.user_group OWNER TO ferry;

--
-- Name: users; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE users (
    uid bigint NOT NULL,
    uname character varying(100) NOT NULL,
    first_name character varying(100),
    middle_name character varying(100),
    last_name character varying(100) NOT NULL,
    status boolean,
    expiration_date date,
    last_updated date DEFAULT ('now'::text)::date NOT NULL
);


ALTER TABLE public.users OWNER TO ferry;

--
-- Name: COLUMN users.uid; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON COLUMN users.uid IS 'unix user id';


--
-- Name: COLUMN users.uname; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON COLUMN users.uname IS 'user unix name';


--
-- Name: COLUMN users.last_name; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON COLUMN users.last_name IS 'user''s last name';


--
-- Name: unitid; Type: DEFAULT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY collaboration_unit ALTER COLUMN unitid SET DEFAULT nextval('experiments_expid_seq'::regclass);


--
-- Name: fqanid; Type: DEFAULT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY grid_fqan ALTER COLUMN fqanid SET DEFAULT nextval('experiment_roles_roleid_seq'::regclass);


--
-- Name: idx_22242_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY collaboration_unit
    ADD CONSTRAINT idx_22242_primary PRIMARY KEY (unitid);


--
-- Name: idx_22246_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY collaboration_unit_group
    ADD CONSTRAINT idx_22246_primary PRIMARY KEY (unitid, groupid);


--
-- Name: idx_22254_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY grid_fqan
    ADD CONSTRAINT idx_22254_primary PRIMARY KEY (fqanid);


--
-- Name: idx_22261_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY compute_access
    ADD CONSTRAINT idx_22261_primary PRIMARY KEY (compid, uid);


--
-- Name: idx_22265_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY storage_resource
    ADD CONSTRAINT idx_22265_primary PRIMARY KEY (storageid);


--
-- Name: idx_22271_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT idx_22271_primary PRIMARY KEY (quotaid);


--
-- Name: idx_22277_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY users
    ADD CONSTRAINT idx_22277_primary PRIMARY KEY (uid);


--
-- Name: idx_22283_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY user_certificate
    ADD CONSTRAINT idx_22283_primary PRIMARY KEY (uid, dn, unitid);


--
-- Name: idx_22287_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY user_group
    ADD CONSTRAINT idx_22287_primary PRIMARY KEY (uid, groupid);


--
-- Name: idx_grid_access; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY grid_access
    ADD CONSTRAINT idx_grid_access PRIMARY KEY (unitid, uid, fqanid);


--
-- Name: idx_groups_gid; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY groups
    ADD CONSTRAINT idx_groups_gid UNIQUE (gid);


--
-- Name: idx_users_uname; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY users
    ADD CONSTRAINT idx_users_uname UNIQUE (uname);


--
-- Name: pk_compute_batch; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY compute_batch
    ADD CONSTRAINT pk_compute_batch PRIMARY KEY (compid, groupid, name);


--
-- Name: pk_compute_resource; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY compute_resource
    ADD CONSTRAINT pk_compute_resource PRIMARY KEY (compid);


--
-- Name: pk_groups; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY groups
    ADD CONSTRAINT pk_groups PRIMARY KEY (groupid);


--
-- Name: pk_user_affiliation; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY user_affiliation
    ADD CONSTRAINT pk_user_affiliation PRIMARY KEY (uid);


--
-- Name: idx_22236_idx_compute_resource_0; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22236_idx_compute_resource_0 ON compute_batch USING btree (groupid);


--
-- Name: idx_22236_idx_compute_resource_1; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22236_idx_compute_resource_1 ON compute_batch USING btree (compid);


--
-- Name: idx_22246_idx_user_group_1; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22246_idx_user_group_1 ON collaboration_unit_group USING btree (unitid);


--
-- Name: idx_22246_idx_user_group_2; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22246_idx_user_group_2 ON collaboration_unit_group USING btree (groupid);


--
-- Name: idx_22249_idx_experiment_membership; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22249_idx_experiment_membership ON grid_access USING btree (uid);


--
-- Name: idx_22249_idx_experiment_membership_0; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22249_idx_experiment_membership_0 ON grid_access USING btree (unitid);


--
-- Name: idx_22249_idx_experiment_membership_1; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22249_idx_experiment_membership_1 ON grid_access USING btree (fqanid);


--
-- Name: idx_22261_fk_interactive_access_groups; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22261_fk_interactive_access_groups ON compute_access USING btree (groupid);


--
-- Name: idx_22261_idx_interactive_access; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22261_idx_interactive_access ON compute_access USING btree (uid);


--
-- Name: idx_22271_idx_quota_0; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22271_idx_quota_0 ON storage_quota USING btree (groupid);


--
-- Name: idx_22271_idx_storage_quota_3; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22271_idx_storage_quota_3 ON storage_quota USING btree (storageid);


--
-- Name: idx_22283_idx_user_certificate; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22283_idx_user_certificate ON user_certificate USING btree (uid);


--
-- Name: idx_22283_idx_user_certificate_0; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22283_idx_user_certificate_0 ON user_certificate USING btree (unitid);


--
-- Name: idx_22283_idx_user_certificate_1; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22283_idx_user_certificate_1 ON user_certificate USING btree (dn);


--
-- Name: idx_22287_idx_user_group; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22287_idx_user_group ON user_group USING btree (uid);


--
-- Name: idx_22287_idx_user_group_0; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22287_idx_user_group_0 ON user_group USING btree (groupid);


--
-- Name: idx_compute_resource; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_compute_resource ON compute_resource USING btree (unitid);


--
-- Name: idx_experiment_fqan; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_experiment_fqan ON grid_fqan USING btree (mapped_group);


--
-- Name: idx_experiment_roles; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_experiment_roles ON grid_fqan USING btree (mapped_user);


--
-- Name: idx_groups_group_name; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE UNIQUE INDEX idx_groups_group_name ON groups USING btree (group_name);


--
-- Name: idx_storage_quota; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_storage_quota ON storage_quota USING btree (uid);


--
-- Name: fk_compute_resource_compute_resource; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY compute_batch
    ADD CONSTRAINT fk_compute_resource_compute_resource FOREIGN KEY (compid) REFERENCES compute_resource(compid);


--
-- Name: fk_compute_resource_experiments; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY compute_resource
    ADD CONSTRAINT fk_compute_resource_experiments FOREIGN KEY (unitid) REFERENCES collaboration_unit(unitid);


--
-- Name: fk_compute_resource_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY compute_batch
    ADD CONSTRAINT fk_compute_resource_groups FOREIGN KEY (groupid) REFERENCES groups(groupid);


--
-- Name: fk_experiment_fqan_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY grid_fqan
    ADD CONSTRAINT fk_experiment_fqan_groups FOREIGN KEY (mapped_group) REFERENCES groups(group_name);


--
-- Name: fk_experiment_fqan_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY grid_fqan
    ADD CONSTRAINT fk_experiment_fqan_users FOREIGN KEY (mapped_user) REFERENCES users(uname);


--
-- Name: fk_experiment_group_experiments; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY collaboration_unit_group
    ADD CONSTRAINT fk_experiment_group_experiments FOREIGN KEY (unitid) REFERENCES collaboration_unit(unitid);


--
-- Name: fk_experiment_group_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY collaboration_unit_group
    ADD CONSTRAINT fk_experiment_group_groups FOREIGN KEY (groupid) REFERENCES groups(groupid);


--
-- Name: fk_experiment_membership_experiment_roles; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY grid_access
    ADD CONSTRAINT fk_experiment_membership_experiment_roles FOREIGN KEY (fqanid) REFERENCES grid_fqan(fqanid);


--
-- Name: fk_experiment_membership_experiments; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY grid_access
    ADD CONSTRAINT fk_experiment_membership_experiments FOREIGN KEY (unitid) REFERENCES collaboration_unit(unitid);


--
-- Name: fk_experiment_membership_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY grid_access
    ADD CONSTRAINT fk_experiment_membership_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_interactive_access_compute_resource; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY compute_access
    ADD CONSTRAINT fk_interactive_access_compute_resource FOREIGN KEY (compid) REFERENCES compute_resource(compid);


--
-- Name: fk_interactive_access_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY compute_access
    ADD CONSTRAINT fk_interactive_access_groups FOREIGN KEY (groupid) REFERENCES groups(groupid);


--
-- Name: fk_interactive_access_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY compute_access
    ADD CONSTRAINT fk_interactive_access_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_storage_quota_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT fk_storage_quota_groups FOREIGN KEY (groupid) REFERENCES groups(groupid);


--
-- Name: fk_storage_quota_resources; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT fk_storage_quota_resources FOREIGN KEY (storageid) REFERENCES storage_resource(storageid);


--
-- Name: fk_storage_quota_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT fk_storage_quota_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_user_affiliation_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY user_affiliation
    ADD CONSTRAINT fk_user_affiliation_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_user_certificate_experiments; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY user_certificate
    ADD CONSTRAINT fk_user_certificate_experiments FOREIGN KEY (unitid) REFERENCES collaboration_unit(unitid);


--
-- Name: fk_user_certificate_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY user_certificate
    ADD CONSTRAINT fk_user_certificate_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_user_group_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY user_group
    ADD CONSTRAINT fk_user_group_groups FOREIGN KEY (groupid) REFERENCES groups(groupid);


--
-- Name: fk_user_group_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY user_group
    ADD CONSTRAINT fk_user_group_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO ferry;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

