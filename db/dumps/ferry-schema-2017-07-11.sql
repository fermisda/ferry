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
-- Name: batch_priority; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE batch_priority (
    uid bigint NOT NULL,
    gid bigint NOT NULL,
    expid bigint NOT NULL,
    resourceid bigint NOT NULL,
    priority bigint NOT NULL,
    last_updated date NOT NULL
);


ALTER TABLE public.batch_priority OWNER TO ferry;

--
-- Name: TABLE batch_priority; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON TABLE batch_priority IS 'table describes condor quota and priority per user';


--
-- Name: condor_quota; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE condor_quota (
    id bigint NOT NULL,
    resourceid bigint DEFAULT 0::bigint NOT NULL,
    uid bigint NOT NULL,
    gid bigint NOT NULL,
    condor_quota character varying(255),
    is_quota_of bigint
);


ALTER TABLE public.condor_quota OWNER TO ferry;

--
-- Name: experiment_group; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE experiment_group (
    expid bigint NOT NULL,
    gid bigint NOT NULL,
    is_primary smallint,
    leader bigint
);


ALTER TABLE public.experiment_group OWNER TO ferry;

--
-- Name: experiment_membership; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE experiment_membership (
    uid bigint NOT NULL,
    expid bigint NOT NULL,
    roleid bigint,
    is_superuser boolean,
    is_banned boolean,
    mapped_uname character varying(100),
    last_updated date NOT NULL,
    mapped_group bigint
);


ALTER TABLE public.experiment_membership OWNER TO ferry;

--
-- Name: experiment_roles; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE experiment_roles (
    roleid bigint NOT NULL,
    role_name character varying(100) NOT NULL
);


ALTER TABLE public.experiment_roles OWNER TO ferry;

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

ALTER SEQUENCE experiment_roles_roleid_seq OWNED BY experiment_roles.roleid;


--
-- Name: experiments; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE experiments (
    expid bigint NOT NULL,
    experiment_name character varying(100) NOT NULL,
    voms_url character varying(200) NOT NULL,
    alternative_name character varying(100),
    last_updated date
);


ALTER TABLE public.experiments OWNER TO ferry;

--
-- Name: TABLE experiments; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON TABLE experiments IS 'experiments and projects';


--
-- Name: COLUMN experiments.expid; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON COLUMN experiments.expid IS 'Fermilab experiment id ';


--
-- Name: COLUMN experiments.voms_url; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON COLUMN experiments.voms_url IS 'url to relevant voms installation. could point to a subgroup within fermilab voms';


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

ALTER SEQUENCE experiments_expid_seq OWNED BY experiments.expid;


--
-- Name: groups; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE groups (
    gid bigint NOT NULL,
    group_name character varying(100) NOT NULL,
    group_type groups_group_type NOT NULL
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
-- Name: interactive_access; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE interactive_access (
    resourceid bigint NOT NULL,
    uid bigint NOT NULL,
    gid bigint NOT NULL,
    expid bigint NOT NULL,
    shell character varying(30) DEFAULT '/bin/bash'::character varying NOT NULL,
    last_updated date NOT NULL
);


ALTER TABLE public.interactive_access OWNER TO ferry;

--
-- Name: resources; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE resources (
    id bigint NOT NULL,
    name character varying(100) NOT NULL,
    path character varying(255),
    shell character varying(255),
    type character varying(255) NOT NULL,
    capacity character varying(100),
    unit character varying(100)
);


ALTER TABLE public.resources OWNER TO ferry;

--
-- Name: storage_quota; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE storage_quota (
    uid bigint,
    gid bigint NOT NULL,
    resourceid bigint,
    is_group boolean,
    path text NOT NULL,
    last_updated date NOT NULL,
    expid bigint NOT NULL,
    shell character varying(255),
    value text NOT NULL,
    unit character varying(100) NOT NULL,
    valid_until date,
    is_quota_of bigint,
    id bigint NOT NULL
);


ALTER TABLE public.storage_quota OWNER TO ferry;

--
-- Name: TABLE storage_quota; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON TABLE storage_quota IS 'table store quota per user in various storages';


--
-- Name: user_affiliation; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE user_affiliation (
    uid bigint NOT NULL,
    affilation_attribute character varying(100) NOT NULL,
    affiliation_value character varying(100)
);


ALTER TABLE public.user_affiliation OWNER TO ferry;

--
-- Name: user_certificate; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE user_certificate (
    uid bigint NOT NULL,
    dn character varying(300) NOT NULL,
    issuer_ca character varying(120) NOT NULL,
    last_update timestamp with time zone DEFAULT now() NOT NULL,
    expid bigint NOT NULL
);


ALTER TABLE public.user_certificate OWNER TO ferry;

--
-- Name: user_group; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE user_group (
    uid bigint NOT NULL,
    gid bigint NOT NULL,
    is_primary boolean
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
    primary_email character varying(30) NOT NULL,
    status boolean,
    expiration_date date,
    last_updated date NOT NULL
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
-- Name: COLUMN users.primary_email; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON COLUMN users.primary_email IS 'user''s preffered email address';


--
-- Name: roleid; Type: DEFAULT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY experiment_roles ALTER COLUMN roleid SET DEFAULT nextval('experiment_roles_roleid_seq'::regclass);


--
-- Name: expid; Type: DEFAULT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY experiments ALTER COLUMN expid SET DEFAULT nextval('experiments_expid_seq'::regclass);


--
-- Name: idx_22233_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY batch_priority
    ADD CONSTRAINT idx_22233_primary PRIMARY KEY (uid, gid, expid, resourceid);


--
-- Name: idx_22236_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY condor_quota
    ADD CONSTRAINT idx_22236_primary PRIMARY KEY (id);


--
-- Name: idx_22242_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY experiments
    ADD CONSTRAINT idx_22242_primary PRIMARY KEY (expid);


--
-- Name: idx_22246_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY experiment_group
    ADD CONSTRAINT idx_22246_primary PRIMARY KEY (expid, gid);


--
-- Name: idx_22254_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY experiment_roles
    ADD CONSTRAINT idx_22254_primary PRIMARY KEY (roleid);


--
-- Name: idx_22258_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY groups
    ADD CONSTRAINT idx_22258_primary PRIMARY KEY (gid);


--
-- Name: idx_22261_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY interactive_access
    ADD CONSTRAINT idx_22261_primary PRIMARY KEY (resourceid, uid, expid);


--
-- Name: idx_22265_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY resources
    ADD CONSTRAINT idx_22265_primary PRIMARY KEY (id);


--
-- Name: idx_22271_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT idx_22271_primary PRIMARY KEY (id);


--
-- Name: idx_22277_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY users
    ADD CONSTRAINT idx_22277_primary PRIMARY KEY (uid);


--
-- Name: idx_22283_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY user_certificate
    ADD CONSTRAINT idx_22283_primary PRIMARY KEY (uid, dn, expid);


--
-- Name: idx_22287_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY user_group
    ADD CONSTRAINT idx_22287_primary PRIMARY KEY (uid, gid);


--
-- Name: idx_experiment_membership; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY experiment_membership
    ADD CONSTRAINT idx_experiment_membership UNIQUE (expid, uid, roleid);


--
-- Name: idx_22233_idx_batch_user_quota; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22233_idx_batch_user_quota ON batch_priority USING btree (uid);


--
-- Name: idx_22233_idx_batch_user_quota_0; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22233_idx_batch_user_quota_0 ON batch_priority USING btree (expid);


--
-- Name: idx_22233_idx_batch_user_quota_1; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22233_idx_batch_user_quota_1 ON batch_priority USING btree (resourceid);


--
-- Name: idx_22233_idx_priority_factor; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22233_idx_priority_factor ON batch_priority USING btree (gid);


--
-- Name: idx_22236_idx_compute_resource; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22236_idx_compute_resource ON condor_quota USING btree (uid);


--
-- Name: idx_22236_idx_compute_resource_0; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22236_idx_compute_resource_0 ON condor_quota USING btree (gid);


--
-- Name: idx_22236_idx_compute_resource_1; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22236_idx_compute_resource_1 ON condor_quota USING btree (resourceid);


--
-- Name: idx_22236_idx_condor_quota; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22236_idx_condor_quota ON condor_quota USING btree (is_quota_of);


--
-- Name: idx_22246_idx_experiment_group; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22246_idx_experiment_group ON experiment_group USING btree (leader);


--
-- Name: idx_22246_idx_user_group_1; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22246_idx_user_group_1 ON experiment_group USING btree (expid);


--
-- Name: idx_22246_idx_user_group_2; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22246_idx_user_group_2 ON experiment_group USING btree (gid);


--
-- Name: idx_22249_idx_experiment_membership; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22249_idx_experiment_membership ON experiment_membership USING btree (uid);


--
-- Name: idx_22249_idx_experiment_membership_0; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22249_idx_experiment_membership_0 ON experiment_membership USING btree (expid);


--
-- Name: idx_22249_idx_experiment_membership_1; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22249_idx_experiment_membership_1 ON experiment_membership USING btree (roleid);


--
-- Name: idx_22258_pk_unix_group; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE UNIQUE INDEX idx_22258_pk_unix_group ON groups USING btree (group_name);


--
-- Name: idx_22261_fk_interactive_access_groups; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22261_fk_interactive_access_groups ON interactive_access USING btree (gid);


--
-- Name: idx_22261_idx_interactive_access; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22261_idx_interactive_access ON interactive_access USING btree (uid);


--
-- Name: idx_22261_idx_interactive_access_1; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22261_idx_interactive_access_1 ON interactive_access USING btree (expid);


--
-- Name: idx_22271_fk_storage_quota_experiments; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22271_fk_storage_quota_experiments ON storage_quota USING btree (expid);


--
-- Name: idx_22271_idx_quota; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22271_idx_quota ON storage_quota USING btree (uid);


--
-- Name: idx_22271_idx_quota_0; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22271_idx_quota_0 ON storage_quota USING btree (gid);


--
-- Name: idx_22271_idx_storage_quota_2; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22271_idx_storage_quota_2 ON storage_quota USING btree (is_quota_of);


--
-- Name: idx_22271_idx_storage_quota_3; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22271_idx_storage_quota_3 ON storage_quota USING btree (resourceid);


--
-- Name: idx_22277_pk_users_0; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE UNIQUE INDEX idx_22277_pk_users_0 ON users USING btree (primary_email);


--
-- Name: idx_22280_idx_user_affiliation; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22280_idx_user_affiliation ON user_affiliation USING btree (uid);


--
-- Name: idx_22283_idx_user_certificate; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22283_idx_user_certificate ON user_certificate USING btree (uid);


--
-- Name: idx_22283_idx_user_certificate_0; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_22283_idx_user_certificate_0 ON user_certificate USING btree (expid);


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

CREATE INDEX idx_22287_idx_user_group_0 ON user_group USING btree (gid);


--
-- Name: fk_compute_resource_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY condor_quota
    ADD CONSTRAINT fk_compute_resource_groups FOREIGN KEY (gid) REFERENCES groups(gid);


--
-- Name: fk_compute_resource_resources; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY condor_quota
    ADD CONSTRAINT fk_compute_resource_resources FOREIGN KEY (resourceid) REFERENCES resources(id);


--
-- Name: fk_compute_resource_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY condor_quota
    ADD CONSTRAINT fk_compute_resource_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_condor_quota_condor_quota; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY condor_quota
    ADD CONSTRAINT fk_condor_quota_condor_quota FOREIGN KEY (is_quota_of) REFERENCES condor_quota(id);


--
-- Name: fk_experiment_group_experiments; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY experiment_group
    ADD CONSTRAINT fk_experiment_group_experiments FOREIGN KEY (expid) REFERENCES experiments(expid);


--
-- Name: fk_experiment_group_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY experiment_group
    ADD CONSTRAINT fk_experiment_group_groups FOREIGN KEY (gid) REFERENCES groups(gid);


--
-- Name: fk_experiment_group_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY experiment_group
    ADD CONSTRAINT fk_experiment_group_users FOREIGN KEY (leader) REFERENCES users(uid);


--
-- Name: fk_experiment_membership_experiment_roles; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY experiment_membership
    ADD CONSTRAINT fk_experiment_membership_experiment_roles FOREIGN KEY (roleid) REFERENCES experiment_roles(roleid);


--
-- Name: fk_experiment_membership_experiments; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY experiment_membership
    ADD CONSTRAINT fk_experiment_membership_experiments FOREIGN KEY (expid) REFERENCES experiments(expid);


--
-- Name: fk_experiment_membership_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY experiment_membership
    ADD CONSTRAINT fk_experiment_membership_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_interactive_access_experiments; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY interactive_access
    ADD CONSTRAINT fk_interactive_access_experiments FOREIGN KEY (expid) REFERENCES experiments(expid);


--
-- Name: fk_interactive_access_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY interactive_access
    ADD CONSTRAINT fk_interactive_access_groups FOREIGN KEY (gid) REFERENCES groups(gid);


--
-- Name: fk_interactive_access_resources; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY interactive_access
    ADD CONSTRAINT fk_interactive_access_resources FOREIGN KEY (resourceid) REFERENCES resources(id);


--
-- Name: fk_interactive_access_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY interactive_access
    ADD CONSTRAINT fk_interactive_access_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_priority_factor_experiments; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY batch_priority
    ADD CONSTRAINT fk_priority_factor_experiments FOREIGN KEY (expid) REFERENCES experiments(expid);


--
-- Name: fk_priority_factor_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY batch_priority
    ADD CONSTRAINT fk_priority_factor_groups FOREIGN KEY (gid) REFERENCES groups(gid);


--
-- Name: fk_priority_factor_resources; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY batch_priority
    ADD CONSTRAINT fk_priority_factor_resources FOREIGN KEY (resourceid) REFERENCES resources(id);


--
-- Name: fk_priority_factor_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY batch_priority
    ADD CONSTRAINT fk_priority_factor_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_storage_quota_experiments; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT fk_storage_quota_experiments FOREIGN KEY (expid) REFERENCES experiments(expid);


--
-- Name: fk_storage_quota_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT fk_storage_quota_groups FOREIGN KEY (gid) REFERENCES groups(gid);


--
-- Name: fk_storage_quota_resources; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT fk_storage_quota_resources FOREIGN KEY (resourceid) REFERENCES resources(id);


--
-- Name: fk_storage_quota_storage_quota; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT fk_storage_quota_storage_quota FOREIGN KEY (is_quota_of) REFERENCES storage_quota(id);


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
    ADD CONSTRAINT fk_user_certificate_experiments FOREIGN KEY (expid) REFERENCES experiments(expid);


--
-- Name: fk_user_certificate_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY user_certificate
    ADD CONSTRAINT fk_user_certificate_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_user_group_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY user_group
    ADD CONSTRAINT fk_user_group_groups FOREIGN KEY (gid) REFERENCES groups(gid);


--
-- Name: fk_user_group_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY user_group
    ADD CONSTRAINT fk_user_group_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: public; Type: ACL; Schema: -; Owner: ferry
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM ferry;
GRANT ALL ON SCHEMA public TO ferry;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

