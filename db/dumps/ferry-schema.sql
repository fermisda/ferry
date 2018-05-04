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
-- Name: affiliation_unit_group; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE affiliation_unit_group (
    unitid integer NOT NULL,
    groupid integer NOT NULL,
    is_primary boolean,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL
);


ALTER TABLE public.affiliation_unit_group OWNER TO ferry;

--
-- Name: affiliation_unit_user_certificate; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE affiliation_unit_user_certificate (
    unitid integer NOT NULL,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date,
    dnid integer NOT NULL
);


ALTER TABLE public.affiliation_unit_user_certificate OWNER TO ferry;

--
-- Name: affiliation_units_unitid_seq; Type: SEQUENCE; Schema: public; Owner: ferry
--

CREATE SEQUENCE affiliation_units_unitid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.affiliation_units_unitid_seq OWNER TO ferry;

--
-- Name: affiliation_units; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE affiliation_units (
    unitid integer DEFAULT nextval('affiliation_units_unitid_seq'::regclass) NOT NULL,
    alternative_name character varying(100),
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL,
    type character varying(100),
    name character varying(100) NOT NULL
);


ALTER TABLE public.affiliation_units OWNER TO ferry;

--
-- Name: TABLE affiliation_units; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON TABLE affiliation_units IS 'experiments and projects';


--
-- Name: COLUMN affiliation_units.unitid; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON COLUMN affiliation_units.unitid IS 'Fermilab collaboration unit id ';


--
-- Name: compute_access; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE compute_access (
    compid integer NOT NULL,
    uid bigint NOT NULL,
    groupid bigint NOT NULL,
    shell character varying(30) DEFAULT '/bin/bash'::character varying NOT NULL,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL,
    home_dir character varying(100)
);


ALTER TABLE public.compute_access OWNER TO ferry;

--
-- Name: compute_batch; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE compute_batch (
    compid integer DEFAULT (0)::bigint NOT NULL,
    groupid integer,
    name character varying(300) NOT NULL,
    value real,
    type character varying(255),
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL,
    valid_until date
);


ALTER TABLE public.compute_batch OWNER TO ferry;

--
-- Name: compute_resources; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE compute_resources (
    compid integer NOT NULL,
    name character varying(100),
    default_shell character varying(100),
    unitid integer,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL,
    default_home_dir character varying(100),
    type character varying(100)
);


ALTER TABLE public.compute_resources OWNER TO ferry;

--
-- Name: compute_resources_compid_seq; Type: SEQUENCE; Schema: public; Owner: ferry
--

CREATE SEQUENCE compute_resources_compid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.compute_resources_compid_seq OWNER TO ferry;

--
-- Name: compute_resources_compid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ferry
--

ALTER SEQUENCE compute_resources_compid_seq OWNED BY compute_resources.compid;


--
-- Name: external_affiliation_attribute; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE external_affiliation_attribute (
    uid bigint NOT NULL,
    attribute character varying(100) NOT NULL,
    value character varying(100),
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL
);


ALTER TABLE public.external_affiliation_attribute OWNER TO ferry;

--
-- Name: grid_access; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE grid_access (
    uid bigint NOT NULL,
    fqanid integer NOT NULL,
    is_superuser boolean,
    is_banned boolean,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL
);


ALTER TABLE public.grid_access OWNER TO ferry;

--
-- Name: grid_fqan_fqanid_seq; Type: SEQUENCE; Schema: public; Owner: ferry
--

CREATE SEQUENCE grid_fqan_fqanid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.grid_fqan_fqanid_seq OWNER TO ferry;

--
-- Name: grid_fqan; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE grid_fqan (
    fqanid integer DEFAULT nextval('grid_fqan_fqanid_seq'::regclass) NOT NULL,
    fqan character varying(300) NOT NULL,
    mapped_user character varying(100),
    mapped_group character varying(100) NOT NULL,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL,
    unitid integer
);


ALTER TABLE public.grid_fqan OWNER TO ferry;

--
-- Name: groups; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE groups (
    groupid integer NOT NULL,
    gid bigint,
    name character varying(100) NOT NULL,
    type groups_group_type NOT NULL,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL
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
-- Name: COLUMN groups.name; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON COLUMN groups.name IS 'unix group name';


--
-- Name: groups_groupid_seq; Type: SEQUENCE; Schema: public; Owner: ferry
--

CREATE SEQUENCE groups_groupid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.groups_groupid_seq OWNER TO ferry;

--
-- Name: groups_groupid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ferry
--

ALTER SEQUENCE groups_groupid_seq OWNED BY groups.groupid;


--
-- Name: nas_storage; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE nas_storage (
    nasid integer NOT NULL,
    server text,
    volume text,
    access_level text,
    host text,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL
);


ALTER TABLE public.nas_storage OWNER TO ferry;

--
-- Name: nas_storage_nasid_seq; Type: SEQUENCE; Schema: public; Owner: ferry
--

CREATE SEQUENCE nas_storage_nasid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.nas_storage_nasid_seq OWNER TO ferry;

--
-- Name: nas_storage_nasid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ferry
--

ALTER SEQUENCE nas_storage_nasid_seq OWNED BY nas_storage.nasid;


--
-- Name: storage_quota; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE storage_quota (
    groupid integer,
    path text,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL,
    value text NOT NULL,
    unit character varying(100) NOT NULL,
    valid_until date,
    quotaid integer NOT NULL,
    storageid integer NOT NULL,
    uid bigint,
    unitid integer
);


ALTER TABLE public.storage_quota OWNER TO ferry;

--
-- Name: TABLE storage_quota; Type: COMMENT; Schema: public; Owner: ferry
--

COMMENT ON TABLE storage_quota IS 'table store quota per user in various storages';


--
-- Name: storage_quota_quotaid_seq; Type: SEQUENCE; Schema: public; Owner: ferry
--

CREATE SEQUENCE storage_quota_quotaid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.storage_quota_quotaid_seq OWNER TO ferry;

--
-- Name: storage_quota_quotaid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ferry
--

ALTER SEQUENCE storage_quota_quotaid_seq OWNED BY storage_quota.quotaid;


--
-- Name: storage_resources; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE storage_resources (
    storageid integer NOT NULL,
    name character varying(100) NOT NULL,
    default_path character varying(255),
    default_quota bigint,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL,
    default_unit character varying(100),
    type character varying(255) NOT NULL
);


ALTER TABLE public.storage_resources OWNER TO ferry;

--
-- Name: storage_resource_storageid_seq; Type: SEQUENCE; Schema: public; Owner: ferry
--

CREATE SEQUENCE storage_resource_storageid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.storage_resource_storageid_seq OWNER TO ferry;

--
-- Name: storage_resource_storageid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ferry
--

ALTER SEQUENCE storage_resource_storageid_seq OWNED BY storage_resources.storageid;


--
-- Name: user_certificates; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE user_certificates (
    dn character varying(300) NOT NULL,
    uid bigint NOT NULL,
    issuer_ca character varying(120) NOT NULL,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL,
    dnid integer NOT NULL
);


ALTER TABLE public.user_certificates OWNER TO ferry;

--
-- Name: user_certificates_dnid_seq; Type: SEQUENCE; Schema: public; Owner: ferry
--

CREATE SEQUENCE user_certificates_dnid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.user_certificates_dnid_seq OWNER TO ferry;

--
-- Name: user_certificates_dnid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ferry
--

ALTER SEQUENCE user_certificates_dnid_seq OWNED BY user_certificates.dnid;


--
-- Name: user_group; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE user_group (
    uid bigint NOT NULL,
    groupid integer NOT NULL,
    is_leader boolean,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL
);


ALTER TABLE public.user_group OWNER TO ferry;

--
-- Name: users; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE users (
    uid bigint NOT NULL,
    uname character varying(100) NOT NULL,
    status boolean,
    expiration_date date,
    last_updated timestamp with time zone DEFAULT ('now'::text)::date NOT NULL,
    full_name character varying(255)
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
-- Name: voms_url; Type: TABLE; Schema: public; Owner: ferry; Tablespace: 
--

CREATE TABLE voms_url (
    unitid integer NOT NULL,
    url character varying(255) NOT NULL,
    last_updated timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.voms_url OWNER TO ferry;

--
-- Name: compid; Type: DEFAULT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY compute_resources ALTER COLUMN compid SET DEFAULT nextval('compute_resources_compid_seq'::regclass);


--
-- Name: groupid; Type: DEFAULT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY groups ALTER COLUMN groupid SET DEFAULT nextval('groups_groupid_seq'::regclass);


--
-- Name: nasid; Type: DEFAULT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY nas_storage ALTER COLUMN nasid SET DEFAULT nextval('nas_storage_nasid_seq'::regclass);


--
-- Name: quotaid; Type: DEFAULT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_quota ALTER COLUMN quotaid SET DEFAULT nextval('storage_quota_quotaid_seq'::regclass);


--
-- Name: storageid; Type: DEFAULT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_resources ALTER COLUMN storageid SET DEFAULT nextval('storage_resource_storageid_seq'::regclass);


--
-- Name: dnid; Type: DEFAULT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY user_certificates ALTER COLUMN dnid SET DEFAULT nextval('user_certificates_dnid_seq'::regclass);


--
-- Name: idx_22254_primary; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY grid_fqan
    ADD CONSTRAINT idx_22254_primary PRIMARY KEY (fqanid);


--
-- Name: pk_affiliation_unit_group; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY affiliation_unit_group
    ADD CONSTRAINT pk_affiliation_unit_group PRIMARY KEY (unitid, groupid);


--
-- Name: pk_affiliation_unit_user_certificate; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY affiliation_unit_user_certificate
    ADD CONSTRAINT pk_affiliation_unit_user_certificate PRIMARY KEY (unitid, dnid);


--
-- Name: pk_affiliation_units; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY affiliation_units
    ADD CONSTRAINT pk_affiliation_units PRIMARY KEY (unitid);


--
-- Name: pk_compute_access; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY compute_access
    ADD CONSTRAINT pk_compute_access PRIMARY KEY (compid, uid);


--
-- Name: pk_compute_batch; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY compute_batch
    ADD CONSTRAINT pk_compute_batch PRIMARY KEY (compid, name);


--
-- Name: pk_compute_resource; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY compute_resources
    ADD CONSTRAINT pk_compute_resource PRIMARY KEY (compid);


--
-- Name: pk_external_affiliation_attribute; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY external_affiliation_attribute
    ADD CONSTRAINT pk_external_affiliation_attribute PRIMARY KEY (uid, attribute);


--
-- Name: pk_grid_access; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY grid_access
    ADD CONSTRAINT pk_grid_access PRIMARY KEY (uid, fqanid);


--
-- Name: pk_groups; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY groups
    ADD CONSTRAINT pk_groups PRIMARY KEY (groupid);


--
-- Name: pk_nas_storage; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY nas_storage
    ADD CONSTRAINT pk_nas_storage PRIMARY KEY (nasid);


--
-- Name: pk_storage_quota; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT pk_storage_quota PRIMARY KEY (quotaid);


--
-- Name: pk_storage_resources; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY storage_resources
    ADD CONSTRAINT pk_storage_resources PRIMARY KEY (storageid);


--
-- Name: pk_user_certificates; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY user_certificates
    ADD CONSTRAINT pk_user_certificates PRIMARY KEY (dnid);


--
-- Name: pk_user_group; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY user_group
    ADD CONSTRAINT pk_user_group PRIMARY KEY (uid, groupid);


--
-- Name: pk_users; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY users
    ADD CONSTRAINT pk_users PRIMARY KEY (uid);


--
-- Name: pk_voms_url; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY voms_url
    ADD CONSTRAINT pk_voms_url PRIMARY KEY (unitid, url);


--
-- Name: unq_affiliation_units_name; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY affiliation_units
    ADD CONSTRAINT unq_affiliation_units_name UNIQUE (name);


--
-- Name: unq_compute_resources_name; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY compute_resources
    ADD CONSTRAINT unq_compute_resources_name UNIQUE (name);


--
-- Name: unq_groups_gid; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY groups
    ADD CONSTRAINT unq_groups_gid UNIQUE (gid);


--
-- Name: unq_groups_group_name; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY groups
    ADD CONSTRAINT unq_groups_group_name UNIQUE (name);


--
-- Name: unq_storage_resources_name; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY storage_resources
    ADD CONSTRAINT unq_storage_resources_name UNIQUE (name);


--
-- Name: unq_user_certificates_dn; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY user_certificates
    ADD CONSTRAINT unq_user_certificates_dn UNIQUE (dn);


--
-- Name: unq_users_uname; Type: CONSTRAINT; Schema: public; Owner: ferry; Tablespace: 
--

ALTER TABLE ONLY users
    ADD CONSTRAINT unq_users_uname UNIQUE (uname);


--
-- Name: idx_affiliation_unit_group_groupid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_affiliation_unit_group_groupid ON affiliation_unit_group USING btree (groupid);


--
-- Name: idx_affiliation_unit_group_unitid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_affiliation_unit_group_unitid ON affiliation_unit_group USING btree (unitid);


--
-- Name: idx_affiliation_unit_user_certificate_dnid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_affiliation_unit_user_certificate_dnid ON affiliation_unit_user_certificate USING btree (dnid);


--
-- Name: idx_affiliation_unit_user_certificate_unitid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_affiliation_unit_user_certificate_unitid ON affiliation_unit_user_certificate USING btree (unitid);


--
-- Name: idx_compute_access_groupid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_compute_access_groupid ON compute_access USING btree (groupid);


--
-- Name: idx_compute_access_uid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_compute_access_uid ON compute_access USING btree (uid);


--
-- Name: idx_compute_batch_compid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_compute_batch_compid ON compute_batch USING btree (compid);


--
-- Name: idx_compute_batch_groupid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_compute_batch_groupid ON compute_batch USING btree (groupid);


--
-- Name: idx_compute_resources_unitid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_compute_resources_unitid ON compute_resources USING btree (unitid);


--
-- Name: idx_grid_access_fqanid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_grid_access_fqanid ON grid_access USING btree (fqanid);


--
-- Name: idx_grid_access_uid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_grid_access_uid ON grid_access USING btree (uid);


--
-- Name: idx_grid_fqan_mapped_group; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_grid_fqan_mapped_group ON grid_fqan USING btree (mapped_group);


--
-- Name: idx_grid_fqan_mapped_user; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_grid_fqan_mapped_user ON grid_fqan USING btree (mapped_user);


--
-- Name: idx_grid_fqan_unitid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_grid_fqan_unitid ON grid_fqan USING btree (unitid);


--
-- Name: idx_storage_quota_groupid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_storage_quota_groupid ON storage_quota USING btree (groupid);


--
-- Name: idx_storage_quota_storageid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_storage_quota_storageid ON storage_quota USING btree (storageid);


--
-- Name: idx_storage_quota_uid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_storage_quota_uid ON storage_quota USING btree (uid);


--
-- Name: idx_storage_quota_unitid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_storage_quota_unitid ON storage_quota USING btree (unitid);


--
-- Name: idx_user_certificates_uid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_user_certificates_uid ON user_certificates USING btree (uid);


--
-- Name: idx_user_group_groupid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_user_group_groupid ON user_group USING btree (groupid);


--
-- Name: idx_user_group_uid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_user_group_uid ON user_group USING btree (uid);


--
-- Name: idx_voms_url_unitid; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE INDEX idx_voms_url_unitid ON voms_url USING btree (unitid);


--
-- Name: unq_affiliation_unit_group_unitid_is_primary; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE UNIQUE INDEX unq_affiliation_unit_group_unitid_is_primary ON affiliation_unit_group USING btree (unitid, is_primary) WHERE (is_primary IS TRUE);


--
-- Name: unq_grid_fqan_fqan_mapped_group; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE UNIQUE INDEX unq_grid_fqan_fqan_mapped_group ON grid_fqan USING btree (fqan, mapped_group) WHERE (mapped_user IS NULL);


--
-- Name: unq_grid_fqan_fqan_mapped_user_mapped_group; Type: INDEX; Schema: public; Owner: ferry; Tablespace: 
--

CREATE UNIQUE INDEX unq_grid_fqan_fqan_mapped_user_mapped_group ON grid_fqan USING btree (fqan, mapped_user, mapped_group);


--
-- Name: fk_affiliation_unit_user_certificate_affiliation_units; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY affiliation_unit_user_certificate
    ADD CONSTRAINT fk_affiliation_unit_user_certificate_affiliation_units FOREIGN KEY (unitid) REFERENCES affiliation_units(unitid);


--
-- Name: fk_affiliation_unit_user_certificate_user_certificates; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY affiliation_unit_user_certificate
    ADD CONSTRAINT fk_affiliation_unit_user_certificate_user_certificates FOREIGN KEY (dnid) REFERENCES user_certificates(dnid);


--
-- Name: fk_compute_resource_affiliation_units; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY compute_resources
    ADD CONSTRAINT fk_compute_resource_affiliation_units FOREIGN KEY (unitid) REFERENCES affiliation_units(unitid);


--
-- Name: fk_compute_resource_compute_resource; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY compute_batch
    ADD CONSTRAINT fk_compute_resource_compute_resource FOREIGN KEY (compid) REFERENCES compute_resources(compid);


--
-- Name: fk_compute_resource_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY compute_batch
    ADD CONSTRAINT fk_compute_resource_groups FOREIGN KEY (groupid) REFERENCES groups(groupid);


--
-- Name: fk_experiment_fqan_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY grid_fqan
    ADD CONSTRAINT fk_experiment_fqan_groups FOREIGN KEY (mapped_group) REFERENCES groups(name);


--
-- Name: fk_experiment_fqan_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY grid_fqan
    ADD CONSTRAINT fk_experiment_fqan_users FOREIGN KEY (mapped_user) REFERENCES users(uname);


--
-- Name: fk_experiment_group_affiliation_units; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY affiliation_unit_group
    ADD CONSTRAINT fk_experiment_group_affiliation_units FOREIGN KEY (unitid) REFERENCES affiliation_units(unitid);


--
-- Name: fk_experiment_group_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY affiliation_unit_group
    ADD CONSTRAINT fk_experiment_group_groups FOREIGN KEY (groupid) REFERENCES groups(groupid);


--
-- Name: fk_grid_access_grid_fqan; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY grid_access
    ADD CONSTRAINT fk_grid_access_grid_fqan FOREIGN KEY (fqanid) REFERENCES grid_fqan(fqanid);


--
-- Name: fk_grid_access_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY grid_access
    ADD CONSTRAINT fk_grid_access_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_grid_fqan_affiliation_units; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY grid_fqan
    ADD CONSTRAINT fk_grid_fqan_affiliation_units FOREIGN KEY (unitid) REFERENCES affiliation_units(unitid);


--
-- Name: fk_interactive_access_compute_resources; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY compute_access
    ADD CONSTRAINT fk_interactive_access_compute_resources FOREIGN KEY (compid) REFERENCES compute_resources(compid);


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
-- Name: fk_storage_quota_affiliation_units; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT fk_storage_quota_affiliation_units FOREIGN KEY (unitid) REFERENCES affiliation_units(unitid);


--
-- Name: fk_storage_quota_groups; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT fk_storage_quota_groups FOREIGN KEY (groupid) REFERENCES groups(groupid);


--
-- Name: fk_storage_quota_resources; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT fk_storage_quota_resources FOREIGN KEY (storageid) REFERENCES storage_resources(storageid);


--
-- Name: fk_storage_quota_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY storage_quota
    ADD CONSTRAINT fk_storage_quota_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_user_affiliation_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY external_affiliation_attribute
    ADD CONSTRAINT fk_user_affiliation_users FOREIGN KEY (uid) REFERENCES users(uid);


--
-- Name: fk_user_certificates_users; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY user_certificates
    ADD CONSTRAINT fk_user_certificates_users FOREIGN KEY (uid) REFERENCES users(uid);


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
-- Name: fk_voms_url_affiliation_units; Type: FK CONSTRAINT; Schema: public; Owner: ferry
--

ALTER TABLE ONLY voms_url
    ADD CONSTRAINT fk_voms_url_affiliation_units FOREIGN KEY (unitid) REFERENCES affiliation_units(unitid);


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

