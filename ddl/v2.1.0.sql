
CREATE SEQUENCE "public".ldap_vopersonid_seq start with 1;

ALTER TYPE external_affiliation_attribute_attribute_type ADD VALUE 'voPersonId';

\i grants.sql

